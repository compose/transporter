package main

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"
)

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

	N int
	// TODO: not implemented
	Interval time.Duration

	mu      sync.Mutex
	failure int
	success int

	cancel func()
}

func (s *stresser) Stress() error {
	cfg := client.Config{
		Endpoints: []string{s.Endpoint},
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			MaxIdleConnsPerHost: s.N,
		},
	}
	c, err := client.New(cfg)
	if err != nil {
		return err
	}

	kv := client.NewKeysAPI(c)
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	for i := 0; i < s.N; i++ {
		go func() {
			for {
				setctx, setcancel := context.WithTimeout(ctx, time.Second)
				key := fmt.Sprintf("foo%d", rand.Intn(s.KeySuffixRange))
				_, err := kv.Set(setctx, key, randStr(s.KeySize), nil)
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

func (s *stresser) Cancel() {
	s.cancel()
}

func (s *stresser) Report() (success int, failure int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.success, s.failure
}

func randStr(size int) string {
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte(int('a') + rand.Intn(26))
	}
	return string(data)
}
