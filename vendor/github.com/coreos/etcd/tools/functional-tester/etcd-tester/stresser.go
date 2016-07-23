// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	clientV2 "github.com/coreos/etcd/client"
	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/transport"
)

func init() {
	grpclog.SetLogger(plog)
}

type stressFunc func(ctx context.Context) error

type stressEntry struct {
	weight float32
	f      stressFunc
}

type stressTable struct {
	entries    []stressEntry
	sumWeights float32
}

func createStressTable(entries []stressEntry) *stressTable {
	st := stressTable{entries: entries}
	for _, entry := range st.entries {
		st.sumWeights += entry.weight
	}
	return &st
}

func (st *stressTable) choose() stressFunc {
	v := rand.Float32() * st.sumWeights
	var sum float32
	var idx int
	for i := range st.entries {
		sum += st.entries[i].weight
		if sum >= v {
			idx = i
			break
		}
	}
	return st.entries[idx].f
}

func newStressPut(kvc pb.KVClient, keySuffixRange, keySize int) stressFunc {
	return func(ctx context.Context) error {
		_, err := kvc.Put(ctx, &pb.PutRequest{
			Key:   []byte(fmt.Sprintf("foo%d", rand.Intn(keySuffixRange))),
			Value: randBytes(keySize),
		}, grpc.FailFast(false))
		return err
	}
}

func newStressRange(kvc pb.KVClient, keySuffixRange int) stressFunc {
	return func(ctx context.Context) error {
		_, err := kvc.Range(ctx, &pb.RangeRequest{
			Key: []byte(fmt.Sprintf("foo%d", rand.Intn(keySuffixRange))),
		}, grpc.FailFast(false))
		return err
	}
}

func newStressRangePrefix(kvc pb.KVClient, keySuffixRange int) stressFunc {
	return func(ctx context.Context) error {
		_, err := kvc.Range(ctx, &pb.RangeRequest{
			Key:      []byte("foo"),
			RangeEnd: []byte(fmt.Sprintf("foo%d", rand.Intn(keySuffixRange))),
		}, grpc.FailFast(false))
		return err
	}
}

func newStressDelete(kvc pb.KVClient, keySuffixRange int) stressFunc {
	return func(ctx context.Context) error {
		_, err := kvc.DeleteRange(ctx, &pb.DeleteRangeRequest{
			Key: []byte(fmt.Sprintf("foo%d", rand.Intn(keySuffixRange))),
		}, grpc.FailFast(false))
		return err
	}
}

func newStressDeletePrefix(kvc pb.KVClient, keySuffixRange int) stressFunc {
	return func(ctx context.Context) error {
		_, err := kvc.DeleteRange(ctx, &pb.DeleteRangeRequest{
			Key:      []byte("foo"),
			RangeEnd: []byte(fmt.Sprintf("foo%d", rand.Intn(keySuffixRange))),
		}, grpc.FailFast(false))
		return err
	}
}

type Stresser interface {
	// Stress starts to stress the etcd cluster
	Stress() error
	// Cancel cancels the stress test on the etcd cluster
	Cancel()
	// Report reports the success and failure of the stress test
	Report() (success int, failure int)
}

type stresser struct {
	Endpoint string

	KeySize        int
	KeySuffixRange int

	qps int
	N   int

	mu sync.Mutex
	wg *sync.WaitGroup

	rateLimiter *rate.Limiter

	cancel func()
	conn   *grpc.ClientConn

	success int

	stressTable *stressTable
}

func (s *stresser) Stress() error {
	// TODO: add backoff option
	conn, err := grpc.Dial(s.Endpoint, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("%v (%s)", err, s.Endpoint)
	}
	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}
	wg.Add(s.N)

	s.mu.Lock()
	s.conn = conn
	s.cancel = cancel
	s.wg = wg
	s.rateLimiter = rate.NewLimiter(rate.Every(time.Second), s.qps)
	s.mu.Unlock()

	kvc := pb.NewKVClient(conn)

	var stressEntries = []stressEntry{
		{weight: 0.7, f: newStressPut(kvc, s.KeySuffixRange, s.KeySize)},
		{weight: 0.07, f: newStressRange(kvc, s.KeySuffixRange)},
		{weight: 0.07, f: newStressRangePrefix(kvc, s.KeySuffixRange)},
		{weight: 0.07, f: newStressDelete(kvc, s.KeySuffixRange)},
		{weight: 0.07, f: newStressDeletePrefix(kvc, s.KeySuffixRange)},
	}
	s.stressTable = createStressTable(stressEntries)

	for i := 0; i < s.N; i++ {
		go s.run(ctx, kvc)
	}

	plog.Printf("stresser %q is started", s.Endpoint)
	return nil
}

func (s *stresser) run(ctx context.Context, kvc pb.KVClient) {
	defer s.wg.Done()

	for {
		if err := s.rateLimiter.Wait(ctx); err == context.Canceled {
			return
		}

		// TODO: 10-second is enough timeout to cover leader failure
		// and immediate leader election. Find out what other cases this
		// could be timed out.
		sctx, scancel := context.WithTimeout(ctx, 10*time.Second)

		err := s.stressTable.choose()(sctx)

		scancel()

		if err != nil {
			shouldContinue := false
			switch grpc.ErrorDesc(err) {
			case context.DeadlineExceeded.Error():
				// This retries when request is triggered at the same time as
				// leader failure. When we terminate the leader, the request to
				// that leader cannot be processed, and times out. Also requests
				// to followers cannot be forwarded to the old leader, so timing out
				// as well. We want to keep stressing until the cluster elects a
				// new leader and start processing requests again.
				shouldContinue = true

			case etcdserver.ErrTimeoutDueToLeaderFail.Error(), etcdserver.ErrTimeout.Error():
				// This retries when request is triggered at the same time as
				// leader failure and follower nodes receive time out errors
				// from losing their leader. Followers should retry to connect
				// to the new leader.
				shouldContinue = true

			case etcdserver.ErrStopped.Error():
				// one of the etcd nodes stopped from failure injection
				shouldContinue = true

			case transport.ErrConnClosing.Desc:
				// server closed the transport (failure injected node)
				shouldContinue = true

			case rpctypes.ErrNotCapable.Error():
				// capability check has not been done (in the beginning)
				shouldContinue = true

				// default:
				// errors from stresser.Cancel method:
				// rpc error: code = 1 desc = context canceled (type grpc.rpcError)
				// rpc error: code = 2 desc = grpc: the client connection is closing (type grpc.rpcError)
			}
			if shouldContinue {
				continue
			}
			return
		}
		s.mu.Lock()
		s.success++
		s.mu.Unlock()
	}
}

func (s *stresser) Cancel() {
	s.mu.Lock()
	s.cancel()
	s.conn.Close()
	wg := s.wg
	s.mu.Unlock()

	wg.Wait()
	plog.Printf("stresser %q is canceled", s.Endpoint)
}

func (s *stresser) Report() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// TODO: find a better way to report v3 tests
	return s.success, -1
}

type stresserV2 struct {
	Endpoint string

	KeySize        int
	KeySuffixRange int

	N int

	mu      sync.Mutex
	failure int
	success int

	cancel func()
}

func (s *stresserV2) Stress() error {
	cfg := clientV2.Config{
		Endpoints: []string{s.Endpoint},
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			MaxIdleConnsPerHost: s.N,
		},
	}
	c, err := clientV2.New(cfg)
	if err != nil {
		return err
	}

	kv := clientV2.NewKeysAPI(c)
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	for i := 0; i < s.N; i++ {
		go func() {
			for {
				setctx, setcancel := context.WithTimeout(ctx, clientV2.DefaultRequestTimeout)
				key := fmt.Sprintf("foo%d", rand.Intn(s.KeySuffixRange))
				_, err := kv.Set(setctx, key, string(randBytes(s.KeySize)), nil)
				setcancel()
				if err == context.Canceled {
					return
				}
				s.mu.Lock()
				if err != nil {
					s.failure++
				} else {
					s.success++
				}
				s.mu.Unlock()
			}
		}()
	}

	<-ctx.Done()
	return nil
}

func (s *stresserV2) Cancel() {
	s.cancel()
}

func (s *stresserV2) Report() (success int, failure int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.success, s.failure
}

func randBytes(size int) []byte {
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte(int('a') + rand.Intn(26))
	}
	return data
}
