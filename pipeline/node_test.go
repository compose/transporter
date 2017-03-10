package pipeline

import (
	"sync"
	"testing"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/message"
)

func TestNodeString(t *testing.T) {
	data := []struct {
		in  *Node
		out string
	}{
		{
			&Node{},
			" - Source:                                                                                                 ",
		},
		{
			NewNode("name", "mongo", adaptor.Config{"uri": "uri", "namespace": "db.col", "debug": false}),
			" - Source:         name                                     mongo           db.col                         uri",
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
			NewNode("first", "mongo", adaptor.Config{}),
			false,
		},
		{
			NewNode("second", "mongo", adaptor.Config{}).Add(NewNode("name", "mongo", adaptor.Config{})),
			true,
		},
		{
			NewNode("third", "mongo", adaptor.Config{}).Add(NewNode("name", "transformer", adaptor.Config{})),
			false,
		},
		{
			NewNode("fourth", "mongo", adaptor.Config{}).Add(NewNode("name", "transformer", adaptor.Config{}).Add(NewNode("name", "mongo", adaptor.Config{}))),
			true,
		},
	}

	for _, v := range data {
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
			NewNode("first", "mongo", adaptor.Config{}),
			"first",
		},
		{
			NewNode("first", "mongo", adaptor.Config{}).Add(NewNode("second", "mongo", adaptor.Config{})),
			"first/second",
		},
		{
			NewNode("first", "mongo", adaptor.Config{}).Add(NewNode("second", "transformer", adaptor.Config{}).Add(NewNode("third", "mongo", adaptor.Config{}))),
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
	Closed bool
}

func (s *StopWriter) Client() (client.Client, error) {
	return &client.Mock{}, nil
}

func (s *StopWriter) Reader() (client.Reader, error) {
	return &client.MockReader{}, nil
}

func (s *StopWriter) Writer(done chan struct{}, wg *sync.WaitGroup) (client.Writer, error) {
	return s, nil
}

func (s *StopWriter) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(client.Session) (message.Msg, error) {
		return msg, nil
	}
}

func (s *StopWriter) Close() {
	s.Closed = true
}

var stopTests = []struct {
	node *Node
}{
	{
		&Node{
			Name:  "starter",
			Type:  "stopWriter",
			Extra: map[string]interface{}{"namespace": "test.test"},
			Children: []*Node{
				&Node{
					Name:     "stopper",
					Type:     "stopWriter",
					Extra:    map[string]interface{}{"namespace": "test.test"},
					Children: nil,
					Parent:   nil,
					done:     make(chan struct{}),
				},
			},
			Parent: nil,
			done:   make(chan struct{}),
		},
	},
}

func TestStop(t *testing.T) {
	for _, st := range stopTests {
		for _, child := range st.node.Children {
			child.Parent = st.node
		}

		if err := st.node.Init(); err != nil {
			t.Errorf("unexpected Init() error, %s", err)
		}
		if err := st.node.Start(); err != nil {
			t.Errorf("unexpected Start() error, %s", err)
		}
		st.node.Stop()
		for _, child := range st.node.Children {
			if !child.w.(*StopWriter).Closed {
				t.Errorf("[%s] child node was not closed but should have been", child.Name)
			}
		}
	}
}
