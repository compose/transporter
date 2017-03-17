package pipeline

import (
	"errors"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/function"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
	"github.com/compose/transporter/pipe"
)

var DefaultNS = regexp.MustCompile(".*")

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
	MsgCount int
	Closed   bool
}

func (s *StopWriter) Client() (client.Client, error) {
	return &client.Mock{}, nil
}

func (s *StopWriter) Reader() (client.Reader, error) {
	return &client.MockReader{MsgCount: 10}, nil
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

var (
	stopTests = []struct {
		node       *Node
		msgCount   int
		applyCount int
	}{
		{
			&Node{
				Name:     "starter",
				Type:     "stopWriter",
				nsFilter: DefaultNS,
				Children: []*Node{
					&Node{
						Name:     "stopper",
						Type:     "stopWriter",
						nsFilter: DefaultNS,
						done:     make(chan struct{}),
					},
				},
				Parent: nil,
				done:   make(chan struct{}),
				pipe:   pipe.NewPipe(nil, "starter"),
			},
			10,
			0,
		},
		{
			&Node{
				Name:     "starter",
				Type:     "stopWriter",
				nsFilter: DefaultNS,
				Children: []*Node{
					&Node{
						Name:       "stopper",
						Type:       "stopWriter",
						nsFilter:   DefaultNS,
						done:       make(chan struct{}),
						Transforms: []*Transform{&Transform{"mock", &function.Mock{}, DefaultNS}},
					},
				},
				Parent: nil,
				done:   make(chan struct{}),
				pipe:   pipe.NewPipe(nil, "starter"),
			},
			10,
			10,
		},
		{
			&Node{
				Name:     "starter",
				Type:     "stopWriter",
				nsFilter: DefaultNS,
				Children: []*Node{
					&Node{
						Name:       "stopper",
						Type:       "stopWriter",
						nsFilter:   DefaultNS,
						done:       make(chan struct{}),
						Transforms: []*Transform{&Transform{"mock", &function.Mock{}, regexp.MustCompile("blah")}},
					},
				},
				Parent: nil,
				done:   make(chan struct{}),
				pipe:   pipe.NewPipe(nil, "starter"),
			},
			10,
			0,
		},
		{
			&Node{
				Name:     "starter",
				Type:     "stopWriter",
				nsFilter: DefaultNS,
				Children: []*Node{
					&Node{
						Name:       "stopper",
						Type:       "stopWriter",
						nsFilter:   DefaultNS,
						done:       make(chan struct{}),
						Transforms: []*Transform{&Transform{"mock", &function.Mock{Err: errors.New("apply failed")}, DefaultNS}},
					},
				},
				Parent: nil,
				done:   make(chan struct{}),
				pipe:   pipe.NewPipe(nil, "starter"),
			},
			0,
			10,
		},
		{
			&Node{
				Name:     "starter",
				Type:     "stopWriter",
				nsFilter: DefaultNS,
				Children: []*Node{
					&Node{
						Name:       "stopper",
						Type:       "stopWriter",
						nsFilter:   DefaultNS,
						done:       make(chan struct{}),
						Transforms: []*Transform{&Transform{"mock", &SkipFunc{}, DefaultNS}},
					},
				},
				Parent: nil,
				done:   make(chan struct{}),
				pipe:   pipe.NewPipe(nil, "starter"),
			},
			0,
			10,
		},
		{
			&Node{
				Name:     "starter",
				Type:     "stopWriter",
				nsFilter: DefaultNS,
				Children: []*Node{
					&Node{
						Name:       "stopper",
						Type:       "stopWriter",
						nsFilter:   DefaultNS,
						done:       make(chan struct{}),
						Transforms: []*Transform{&Transform{"mock", &SkipFunc{UsingOp: true}, DefaultNS}},
					},
				},
				Parent: nil,
				done:   make(chan struct{}),
				pipe:   pipe.NewPipe(nil, "starter"),
			},
			0,
			10,
		},
	}
)

func TestStop(t *testing.T) {
	for _, st := range stopTests {
		s := &StopWriter{}
		st.node.c, _ = s.Client()
		for _, child := range st.node.Children {
			child.c, _ = s.Client()
			child.writer, _ = s.Writer(child.done, &child.wg)
			child.pipe = pipe.NewPipe(st.node.pipe, "stopper")
			child.Parent = st.node
		}
		st.node.reader, _ = s.Reader()
		if err := st.node.Start(); err != nil {
			t.Errorf("unexpected Start() error, %s", err)
		}
		time.Sleep(1 * time.Second)
		st.node.Stop()
		for _, child := range st.node.Children {
			if !s.Closed {
				t.Errorf("[%s] child node was not closed but should have been", child.Name)
			}
		}
		if st.msgCount != s.MsgCount {
			t.Errorf("wrong number of messages received, expected %d, got %d", st.msgCount, s.MsgCount)
		}
		if len(st.node.Children[0].Transforms) > 0 {
			switch mock := st.node.Children[0].Transforms[0].Fn.(type) {
			case *function.Mock:
				if mock.ApplyCount != st.applyCount {
					t.Errorf("wrong number of transforms applied, expected %d, got %d", st.applyCount, mock.ApplyCount)
				}
			}
		}
	}
}
