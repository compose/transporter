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
		in  *Node
		out string
	}{
		{
			&Node{
				Name:     "name",
				Type:     "mongodb",
				nsFilter: DefaultNS,
			},
			" - Source:         name                                     mongodb         .*                            ",
		},
	}

	for _, v := range data {
		if v.in.String() != v.out {
			t.Errorf("\nexpected: '%s'\n     got: '%s'\n", v.out, v.in.String())
		}
	}
}

func TestValidate(t *testing.T) {
	data := []struct {
		in  *Node
		out bool
	}{
		{
			&Node{Name: "first", Type: "mongodb", nsFilter: DefaultNS, Parent: nil},
			false,
		},
		{
			&Node{Name: "first", Type: "mongodb", nsFilter: DefaultNS, Parent: nil,
				Children: []*Node{
					&Node{Name: "second", Type: "mongodb", nsFilter: DefaultNS},
				},
			},
			true,
		},
	}

	for _, v := range data {
		for _, child := range v.in.Children {
			child.Parent = v.in
		}
		if v.in.Validate() != v.out {
			t.Errorf("%s: expected: %t got: %t", v.in.Name, v.out, v.in.Validate())
		}
	}
}

func TestPath(t *testing.T) {
	data := []struct {
		in  *Node
		out string
	}{
		{
			&Node{Name: "first", Type: "mongodb", nsFilter: DefaultNS},
			"first",
		},
		{
			&Node{Name: "second", Type: "mongodb", nsFilter: DefaultNS, Parent: &Node{Name: "first", Type: "mongodb", nsFilter: DefaultNS}},
			"first/second",
		},
		{
			&Node{Name: "third", Type: "mongodb", nsFilter: DefaultNS, Parent: &Node{Name: "second", Type: "transformer", nsFilter: DefaultNS, Parent: &Node{Name: "first", Type: "mongodb", nsFilter: DefaultNS}}},
			"first/second/third",
		},
	}

	for _, v := range data {
		node := v.in
		var path string
		for {
			if len(node.Children) == 0 {
				path = node.Path()
				break
			}
			node = node.Children[0]
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
	SendCount int
	MsgCount  int
	Closed    bool
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
		return msg, nil
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
		node       func() (*Node, *StopWriter, func())
		msgCount   int
		applyCount int
		startErr   error
	}{
		{
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
						CommitDelay: 1 * time.Second,
					}),
				)
				return n, a, func() {}
			},
			4, 0, nil,
		},
		{
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
							"MyCollection":      104014,
							"anotherCollection": 2998,
							"testC":             1,
						},
						CommitDelay: 10 * time.Second,
					}),
				)
				return n, a, func() {}
			},
			2, 0, ErrResumeTimedOut,
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
			t.Errorf("unexpected Start() error, expected %s, got %s", st.startErr, err)
		}
		if !errored {
			source.Stop()
			close(stopC)
		}
		<-stopC
		for _, child := range source.Children {
			if !s.Closed {
				t.Errorf("[%s] child node was not closed but should have been", child.Name)
			}
		}
		if st.msgCount != s.MsgCount {
			t.Errorf("wrong number of messages received, expected %d, got %d", st.msgCount, s.MsgCount)
		}
		if len(source.Children[0].Transforms) > 0 {
			switch mock := source.Children[0].Transforms[0].Fn.(type) {
			case *function.Mock:
				if mock.ApplyCount != st.applyCount {
					t.Errorf("wrong number of transforms applied, expected %d, got %d", st.applyCount, mock.ApplyCount)
				}
			}
		}
	}
}
