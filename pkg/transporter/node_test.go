package transporter

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/pipe"
)

func TestNodeString(t *testing.T) {
	data := []struct {
		in  *Node
		out string
	}{
		{
			&Node{},
			"                   Name                                     Type            Namespace                      Uri\n - Source:                                                                  no namespace set               no uri set",
		},
		{
			NewNode("name", "mongodb", map[string]interface{}{"namespace": "ns", "uri": "uri"}),
			"                   Name                                     Type            Namespace                      Uri\n - Source:         name                                     mongodb         ns                             uri",
		},
	}

	for _, v := range data {
		if v.in.String() != v.out {
			t.Errorf("\nexpected:\n%s\ngot:\n%s\n", v.out, v.in.String())
		}
	}
}

var anError = errors.New("this is an error")

type Impl struct {
	value string
}

func NewImpl(p *pipe.Pipe, extra map[string]interface{}) (*Impl, error) {
	val, ok := extra["value"]
	if !ok {
		return nil, anError
	}
	return &Impl{value: val.(string)}, nil
}

func TestNodeCreateImpl(t *testing.T) {
	nodeRegistry["source"] = NewTestSourceImpl

	data := []struct {
		in  *Node
		out *TestSourceImpl
		err error
	}{
		{
			&Node{Name: "dumbname", Type: "source", Extra: map[string]interface{}{"value": "rockettes"}},
			&TestSourceImpl{value: "rockettes"},
			nil,
		},
		{
			&Node{Name: "dumnname", Type: "source", Extra: map[string]interface{}{"blah": "rockettes"}},
			&TestSourceImpl{},
			anError,
		},
	}
	for _, v := range data {
		err := v.in.createImpl(pipe.NewPipe(nil, v.in.Name, 1*time.Second))

		if err != v.err {
			t.Errorf("\nexpected error:\n%v\ngot error:\n%v\n", v.err, err)
			t.FailNow()
		}
		if !reflect.DeepEqual(v.out, v.in.impl) && err == nil {
			t.Errorf("%s\nexpected:\n%+v\ngot:\n%+v\n", v.in.Name, v.out, v.in.impl)
		}
	}
}

// a random type that implements the source interface
type TestSourceImpl struct {
	value string
}

func NewTestSourceImpl(p *pipe.Pipe, extra map[string]interface{}) (*TestSourceImpl, error) {
	val, ok := extra["value"]
	if !ok {
		return nil, anError
	}
	return &TestSourceImpl{value: val.(string)}, nil
}

func (s *TestSourceImpl) Stop() error {
	return nil
}

func (s *TestSourceImpl) Start() error {
	return nil
}

func (s *TestSourceImpl) Listen() error {
	return nil
}

// a random type that implements the node interface
type TestNodeImpl struct {
	value string
}

func NewTestNodeImpl(p *pipe.Pipe, extra map[string]interface{}) (*TestNodeImpl, error) {
	val, ok := extra["value"]
	if !ok {
		return nil, anError
	}
	return &TestNodeImpl{value: val.(string)}, nil
}

func (s *TestNodeImpl) Start() error {
	return nil
}

func (s *TestNodeImpl) Stop() error {
	return nil
}

func (s *TestNodeImpl) Listen() error {
	return nil
}

func TestNodeInit(t *testing.T) {
	nodeRegistry["node"] = NewTestNodeImpl
	nodeRegistry["notasource"] = NewImpl

	data := []struct {
		in  *Node
		out *TestNodeImpl
		err string
	}{
		{
			NewNode("somenode", "node", map[string]interface{}{"value": "rockettes"}),
			&TestNodeImpl{value: "rockettes"},
			"",
		},
		{
			NewNode("morenode", "node", map[string]interface{}{"blah": "rockettes"}),
			nil,
			"this is an",
		},
		{
			NewNode("notanode", "notasource", map[string]interface{}{"value": "rockettes"}),
			nil,
			"cannot cre",
		},
		{
			NewNode("notasource", "notasource", map[string]interface{}{"blah": "rockettes"}),
			nil,
			"this is an",
		},
		{
			NewNode("pickl", "notapickle", map[string]interface{}{"blah": "rockettes"}),
			nil,
			"Node not d",
		},
	}
	for _, v := range data {
		err := v.in.Init(testEmptyApiConfig)

		if err != nil && err.Error()[:10] != v.err {
			t.Errorf("expected error %v, got %v", v.err, err.Error()[:10])
			continue
		}
		if !reflect.DeepEqual(v.out, v.in.impl) && err == nil {
			t.Errorf("expected (%T)%+v, got (%T)%+v", v.out, v.out, v.in.impl, v.in.impl)
		}
	}
}
