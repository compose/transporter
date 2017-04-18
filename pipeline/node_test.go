package pipeline

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/function"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
	"github.com/compose/transporter/offset"
)

var (
	DefaultNS       = regexp.MustCompile(".*")
	defaultNsString = "/.*/"
)

func TestNodeString(t *testing.T) {
	data := []struct {
		in  func() *Node
		out string
	}{
		{
			func() *Node {
				n, _ := NewNodeWithOptions("name", "mongodb", defaultNsString)
				return n
			},
			" - Source:         name                                     mongodb         .*                            ",
		},
	}

	for _, v := range data {
		n := v.in()
		if n.String() != v.out {
			t.Errorf("\nexpected: '%s'\n     got: '%s'\n", v.out, n.String())
		}
	}
}

func TestValidate(t *testing.T) {
	data := []struct {
		in  func() *Node
		out bool
	}{
		{
			func() *Node {
				n, _ := NewNodeWithOptions("first", "mongodb", defaultNsString)
				return n
			},
			false,
		},
		{
			func() *Node {
				source, _ := NewNodeWithOptions("first", "mongodb", defaultNsString)
				NewNodeWithOptions("second", "mongodb", defaultNsString, WithParent(source))
				return source
			},
			true,
		},
	}

	for _, v := range data {
		node := v.in()
		if node.Validate() != v.out {
			t.Errorf("%s: expected: %t got: %t", node.Name, v.out, node.Validate())
		}
	}
}

func TestPath(t *testing.T) {
	data := []struct {
		in  func() *Node
		out string
	}{
		{
			func() *Node {
				n, _ := NewNodeWithOptions("first", "mongodb", defaultNsString)
				return n
			},
			"first",
		},
		{
			func() *Node {
				first, _ := NewNodeWithOptions("first", "mongodb", defaultNsString)
				second, _ := NewNodeWithOptions("second", "mongodb", defaultNsString, WithParent(first))
				return second
			},
			"first/second",
		},
		{
			func() *Node {
				first, _ := NewNodeWithOptions("first", "mongodb", defaultNsString)
				second, _ := NewNodeWithOptions("second", "mongodb", defaultNsString, WithParent(first))
				third, _ := NewNodeWithOptions("third", "mongodb", defaultNsString, WithParent(second))
				return third
			},
			"first/second/third",
		},
	}

	for _, v := range data {
		node := v.in()
		var path string
		for {
			if len(node.children) == 0 {
				path = node.path
				break
			}
			node = node.children[0]
		}
		if path != v.out {
			t.Errorf("%s: expected: %s got: %s", node.Name, v.out, path)
		}
	}
}

func init() {
	adaptor.Add("stopWriter", func() adaptor.Adaptor {
		return &StopWriter{}
	})
}

type StopWriter struct {
	SendCount    int
	MsgCount     int
	ConfirmDelay time.Duration
	WriteErr     error
	Closed       bool
}

func (s *StopWriter) Client() (client.Client, error) {
	return &client.Mock{}, nil
}

func (s *StopWriter) Reader() (client.Reader, error) {
	return &client.MockReader{MsgCount: s.SendCount}, nil
}

func (s *StopWriter) Writer(done chan struct{}, wg *sync.WaitGroup) (client.Writer, error) {
	return s, nil
}

func (s *StopWriter) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(client.Session) (message.Msg, error) {
		s.MsgCount++
		if s.ConfirmDelay > 0 {
			go func(m message.Msg) {
				time.Sleep(s.ConfirmDelay)
				close(m.Confirms())
			}(msg)
			return msg, nil
		}
		if s.WriteErr == nil {
			close(msg.Confirms())
		}
		return msg, s.WriteErr
	}
}

func (s *StopWriter) Close() {
	s.Closed = true
}

type SkipFunc struct {
	UsingOp bool
}

func (s *SkipFunc) Apply(msg message.Msg) (message.Msg, error) {
	if s.UsingOp {
		return message.From(ops.Skip, msg.Namespace(), msg.Data()), nil
	}
	return nil, nil
}

func scratchDataDir(suffix string) string {
	rand.Seed(time.Now().Unix())
	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("nodetest_%s_%d", suffix, rand.Int31()))
	os.MkdirAll(dataDir, 0777)
	return dataDir
}

var (
	stopTests = []struct {
		name       string
		node       func() (*Node, *StopWriter, func())
		msgCount   int
		applyCount int
		startErr   error
	}{
		{
			"base",
			func() (*Node, *StopWriter, func()) {
				dataDir := scratchDataDir("base")
				a := &StopWriter{SendCount: 10}
				n, _ := NewNodeWithOptions(
					"starter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog(dataDir, 1024),
				)
				om, _ := offset.NewLogManager(dataDir, "stopper")
				NewNodeWithOptions(
					"stopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithOffsetManager(om),
				)
				return n, a, func() { os.RemoveAll(dataDir) }
			},
			10, 0, nil,
		},
		{
			"with_transform",
			func() (*Node, *StopWriter, func()) {
				dataDir := scratchDataDir("mocktransform")
				a := &StopWriter{SendCount: 10}
				n, _ := NewNodeWithOptions(
					"starter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog(dataDir, 1024),
				)
				om, _ := offset.NewLogManager(dataDir, "stopper")
				NewNodeWithOptions(
					"stopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithTransforms([]*Transform{&Transform{"mock", &function.Mock{}, DefaultNS}}),
					WithOffsetManager(om),
				)
				return n, a, func() { os.RemoveAll(dataDir) }
			},
			10, 10, nil,
		},
		{
			"transform_ns_mismatch",
			func() (*Node, *StopWriter, func()) {
				dataDir := scratchDataDir("mocktransform_ns_mismatch")
				a := &StopWriter{SendCount: 10}
				n, _ := NewNodeWithOptions(
					"starter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog(dataDir, 1024),
				)
				om, _ := offset.NewLogManager(dataDir, "stopper")
				NewNodeWithOptions(
					"stopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithTransforms([]*Transform{
						&Transform{
							"mock",
							&function.Mock{},
							regexp.MustCompile("blah"),
						},
					}),
					WithOffsetManager(om),
				)
				return n, a, func() { os.RemoveAll(dataDir) }
			},
			10, 0, nil,
		},
		{
			"with_transform_err",
			func() (*Node, *StopWriter, func()) {
				dataDir := scratchDataDir("mocktransform_err")
				a := &StopWriter{SendCount: 10}
				n, _ := NewNodeWithOptions(
					"starter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog(dataDir, 1024),
				)
				om, _ := offset.NewLogManager(dataDir, "stopper")
				NewNodeWithOptions(
					"stopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithTransforms([]*Transform{
						&Transform{
							"mock",
							&function.Mock{Err: errors.New("apply failed")},
							DefaultNS,
						},
					}),
					WithOffsetManager(om),
				)
				return n, a, func() { os.RemoveAll(dataDir) }
			},
			0, 1, nil,
		},
		{
			"with_skip_transform",
			func() (*Node, *StopWriter, func()) {
				dataDir := scratchDataDir("skiptransform")
				a := &StopWriter{SendCount: 10}
				n, _ := NewNodeWithOptions(
					"starter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog(dataDir, 1024),
				)
				om, _ := offset.NewLogManager(dataDir, "stopper")
				NewNodeWithOptions(
					"stopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithTransforms([]*Transform{
						&Transform{
							"mock",
							&SkipFunc{},
							DefaultNS,
						},
					}),
					WithOffsetManager(om),
				)
				return n, a, func() { os.RemoveAll(dataDir) }
			},
			0, 10, nil,
		},
		{
			"with_skip_op_transform",
			func() (*Node, *StopWriter, func()) {
				dataDir := scratchDataDir("skipop_transform")
				a := &StopWriter{SendCount: 10}
				n, _ := NewNodeWithOptions(
					"starter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog(dataDir, 1024),
				)
				om, _ := offset.NewLogManager(dataDir, "stopper")
				NewNodeWithOptions(
					"stopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithTransforms([]*Transform{
						&Transform{
							"mock",
							&SkipFunc{UsingOp: true},
							DefaultNS,
						},
					}),
					WithOffsetManager(om),
				)
				return n, a, func() { os.RemoveAll(dataDir) }
			},
			0, 10, nil,
		},
		{
			"resume_from_zero",
			func() (*Node, *StopWriter, func()) {
				dataDir := scratchDataDir("resume_from_zero")
				a := &StopWriter{}
				n, _ := NewNodeWithOptions(
					"starter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog("testdata/restart_from_zero", 1024),
				)
				om, _ := offset.NewLogManager(dataDir, "stopper")
				NewNodeWithOptions(
					"stopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithOffsetManager(om),
				)
				return n, a, func() { os.RemoveAll(dataDir) }
			},
			104016, 0, nil,
		},
		{
			"resume_from_middle",
			func() (*Node, *StopWriter, func()) {
				os.Remove("testdata/restart_from_middle/__consumer_offsets-stopper/00000000000000000000.log")
				in, _ := os.Open("testdata/restart_from_middle/copy_from_here/00000000000000000000.log")
				os.MkdirAll("testdata/restart_from_middle/__consumer_offsets-stopper", 0777)
				out, _ := os.Create("testdata/restart_from_middle/__consumer_offsets-stopper/00000000000000000000.log")
				io.Copy(out, in)
				defer func() {
					out.Close()
					in.Close()
					os.Remove("testdata/restart_from_middle/__consumer_offsets-stopper/00000000000000000000.log")
				}()

				a := &StopWriter{}
				n, _ := NewNodeWithOptions(
					"starter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog("testdata/restart_from_middle", 1024),
				)
				om, _ := offset.NewLogManager("testdata/restart_from_middle", "stopper")
				NewNodeWithOptions(
					"stopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithOffsetManager(om),
				)
				return n, a, func() {}
			},
			2001, 0, nil,
		},
		{
			"resume_from_end",
			func() (*Node, *StopWriter, func()) {
				a := &StopWriter{}
				n, _ := NewNodeWithOptions(
					"starter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog("testdata/restart_from_end", 1024),
				)
				om, _ := offset.NewLogManager("testdata/restart_from_end", "stopper")
				NewNodeWithOptions(
					"stopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithOffsetManager(om),
				)
				return n, a, func() {}
			},
			0, 0, nil,
		},
		{
			"resume_from_zero_mock_offset",
			func() (*Node, *StopWriter, func()) {
				a := &StopWriter{}
				n, _ := NewNodeWithOptions(
					"starter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog("testdata/restart_from_zero", 1024),
				)
				NewNodeWithOptions(
					"stopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithOffsetManager(&offset.MockManager{
						MemoryMap: map[string]uint64{},
					}),
				)
				return n, a, func() {}
			},
			104016, 0, nil,
		},
		{
			"resume_multi_ns_offset",
			func() (*Node, *StopWriter, func()) {
				a := &StopWriter{}
				n, _ := NewNodeWithOptions(
					"starter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog("testdata/restart_from_zero", 1024),
				)
				NewNodeWithOptions(
					"stopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithOffsetManager(&offset.MockManager{
						MemoryMap: map[string]uint64{
							"MyCollection":      104015,
							"anotherCollection": 2998,
							"testC":             1,
						},
					}),
				)
				return n, a, func() {}
			},
			0, 0, nil,
		},
		{
			"resume_multi_ns_offset_with_messages",
			func() (*Node, *StopWriter, func()) {
				a := &StopWriter{}
				n, _ := NewNodeWithOptions(
					"starter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog("testdata/restart_from_zero", 1024),
				)
				NewNodeWithOptions(
					"stopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithOffsetManager(&offset.MockManager{
						MemoryMap: map[string]uint64{
							"MyCollection":      104012,
							"anotherCollection": 2998,
							"testC":             1,
						},
					}),
				)
				return n, a, func() {}
			},
			4, 0, nil,
		},
		{
			"resume_with_multi_ns_delayed_commit",
			func() (*Node, *StopWriter, func()) {
				a := &StopWriter{}
				n, _ := NewNodeWithOptions(
					"starter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog("testdata/restart_from_zero", 1024),
				)
				NewNodeWithOptions(
					"stopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithResumeTimeout(2*time.Second),
					WithOffsetManager(&offset.MockManager{
						MemoryMap: map[string]uint64{
							"MyCollection":      104013,
							"anotherCollection": 2998,
							"testC":             1,
						},
						CommitDelay: 10 * time.Second,
					}),
				)
				return n, a, func() {}
			},
			3, 0, ErrResumeTimedOut,
		},
		{
			"with_ns_filter",
			func() (*Node, *StopWriter, func()) {
				a := &StopWriter{}
				n, _ := NewNodeWithOptions(
					"starter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog("testdata/pipeline_run", 1024),
				)
				NewNodeWithOptions(
					"stopper", "stopWriter", "/blah/",
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithOffsetManager(&offset.MockManager{MemoryMap: map[string]uint64{}}),
				)
				return n, a, func() {}
			},
			0, 0, nil,
		},
		{
			"with_ctx_timeout",
			func() (*Node, *StopWriter, func()) {
				a := &StopWriter{
					ConfirmDelay: 11 * time.Second,
				}
				n, _ := NewNodeWithOptions(
					"ctxStarter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog("testdata/pipeline_run", 1024),
				)
				NewNodeWithOptions(
					"ctxStopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithResumeTimeout(15*time.Second),
					WithOffsetManager(&offset.MockManager{MemoryMap: map[string]uint64{}}),
				)
				return n, a, func() {}
			},
			9, 0, ErrResumeTimedOut,
		},
		{
			"with_ctx_cancel",
			func() (*Node, *StopWriter, func()) {
				a := &StopWriter{
					WriteErr: errors.New("bad write"),
				}
				n, _ := NewNodeWithOptions(
					"ctx_cancel_starter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog("testdata/pipeline_run", 1024),
				)
				NewNodeWithOptions(
					"ctx_cancel_stopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithResumeTimeout(2*time.Second),
					WithOffsetManager(&offset.MockManager{MemoryMap: map[string]uint64{}}),
				)
				return n, a, func() {}
			},
			1, 0, ErrResumeStopped,
		},
		{
			"with_offset_commit_error",
			func() (*Node, *StopWriter, func()) {
				a := &StopWriter{}
				n, _ := NewNodeWithOptions(
					"offset_commit_err_starter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog("testdata/pipeline_run", 1024),
				)
				NewNodeWithOptions(
					"offset_commit_err_stopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithResumeTimeout(2*time.Second),
					WithOffsetManager(&offset.MockManager{
						MemoryMap: map[string]uint64{},
						CommitErr: errors.New("failed to commit offset"),
					}),
				)
				return n, a, func() {}
			},
			9, 0, ErrResumeTimedOut,
		},
	}
)

func TestStop(t *testing.T) {
	for _, st := range stopTests {
		source, s, deferFunc := st.node()
		defer deferFunc()
		var errored bool
		stopC := make(chan struct{})
		go func() {
			select {
			case <-source.pipe.Err:
				errored = true
				source.Stop()
				close(stopC)
			}
		}()
		if err := source.Start(); err != st.startErr {
			t.Errorf("[%s] unexpected Start() error, expected %s, got %s", st.name, st.startErr, err)
		}
		if !errored {
			source.Stop()
			close(stopC)
		}
		<-stopC
		for _, child := range source.children {
			if !s.Closed {
				t.Errorf("[%s] child node was not closed but should have been", child.Name)
			}
		}
		if st.msgCount != s.MsgCount {
			t.Errorf("[%s] wrong number of messages received, expected %d, got %d", st.name, st.msgCount, s.MsgCount)
		}
		if len(source.children[0].transforms) > 0 {
			switch mock := source.children[0].transforms[0].Fn.(type) {
			case *function.Mock:
				if mock.ApplyCount != st.applyCount {
					t.Errorf("[%s] wrong number of transforms applied, expected %d, got %d", st.name, st.applyCount, mock.ApplyCount)
				}
			}
		}
	}
}
