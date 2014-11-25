package transporter

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/pipe"
)

func TestConfigNodeString(t *testing.T) {
	data := []struct {
		in  ConfigNode
		out string
	}{
		{
			ConfigNode{},
			"                                     no namespace set               no uri set",
		},
		{
			ConfigNode{Name: "name", Type: "mongodb", Extra: map[string]interface{}{"namespace": "ns", "uri": "uri"}},
			"name                 mongodb         ns                             uri",
		},
	}

	for _, v := range data {
		if v.in.String() != v.out {
			t.Errorf("expected %s, got %s", v.out, v.in.String())
		}
	}
}

var anError = errors.New("this is an error")

type Impl struct {
	value string
}

func NewImpl(p pipe.Pipe, extra map[string]interface{}) (*Impl, error) {
	val, ok := extra["value"]
	if !ok {
		return nil, anError
	}
	return &Impl{value: val.(string)}, nil
}

func TestConfigNodeCallCreator(t *testing.T) {
	p := pipe.NewSourcePipe("name", 1*time.Second)
	data := []struct {
		in  ConfigNode
		out *Impl
		err error
	}{
		{
			ConfigNode{Extra: map[string]interface{}{"value": "rockettes"}},
			&Impl{value: "rockettes"},
			nil,
		},
		{
			ConfigNode{Extra: map[string]interface{}{"blah": "rockettes"}},
			&Impl{},
			anError,
		},
	}
	for _, v := range data {
		val, err := v.in.callCreator(p, NewImpl)

		if err != v.err {
			t.Errorf("expected error %v, got %v", v.err, err)
			t.FailNow()
		}
		if !reflect.DeepEqual(v.out, val.Interface().(*Impl)) && err == nil {
			t.Errorf("expected %v, got %v", v.out, val.Interface().(*Impl))
		}
	}
}

// a random type that implements the source interface
type TestSourceImpl struct {
	value string
}

func NewTestSourceImpl(p pipe.Pipe, extra map[string]interface{}) (*TestSourceImpl, error) {
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

// a random type that implements the node interface
type TestNodeImpl struct {
	value string
}

func NewTestNodeImpl(p pipe.Pipe, extra map[string]interface{}) (*TestNodeImpl, error) {
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

func TestConfigNodeCreateSource(t *testing.T) {
	p := pipe.NewSourcePipe("name", 1*time.Second)

	sourceRegistry["source"] = NewTestSourceImpl
	sourceRegistry["notasource"] = NewImpl

	data := []struct {
		in  ConfigNode
		out *TestSourceImpl
		err string
	}{
		{
			ConfigNode{Type: "source", Extra: map[string]interface{}{"value": "rockettes"}},
			&TestSourceImpl{value: "rockettes"},
			"",
		},
		{
			ConfigNode{Type: "source", Extra: map[string]interface{}{"blah": "rockettes"}},
			nil,
			"this is an",
		},
		{
			ConfigNode{Type: "notasource", Extra: map[string]interface{}{"value": "rockettes"}},
			nil,
			"cannot cre",
		},
		{
			ConfigNode{Type: "notasource", Extra: map[string]interface{}{"blah": "rockettes"}},
			nil,
			"this is an",
		},
		{
			ConfigNode{Type: "notaevenlisted", Extra: map[string]interface{}{"blah": "rockettes"}},
			nil,
			"Node not d",
		},
	}
	for _, v := range data {
		val, err := v.in.CreateSource(p)

		if err != nil && err.Error()[:10] != v.err {
			t.Errorf("expected error %v, got %v", v.err, err.Error()[:10])
			continue
		}
		if !reflect.DeepEqual(v.out, val) && err == nil {
			t.Errorf("expected (%T)%+v, got (%T)%+v", v.out, v.out, val, val)
		}
	}
}

func TestConfigNodeCreate(t *testing.T) {
	p := pipe.NewSourcePipe("name", 1*time.Second)

	nodeRegistry["node"] = NewTestNodeImpl
	nodeRegistry["notasource"] = NewImpl

	data := []struct {
		in  ConfigNode
		out *TestNodeImpl
		err string
	}{
		{
			ConfigNode{Type: "node", Extra: map[string]interface{}{"value": "rockettes"}},
			&TestNodeImpl{value: "rockettes"},
			"",
		},
		{
			ConfigNode{Type: "node", Extra: map[string]interface{}{"blah": "rockettes"}},
			nil,
			"this is an",
		},
		{
			ConfigNode{Type: "notasource", Extra: map[string]interface{}{"value": "rockettes"}},
			nil,
			"cannot cre",
		},
		{
			ConfigNode{Type: "notasource", Extra: map[string]interface{}{"blah": "rockettes"}},
			nil,
			"this is an",
		},
		{
			ConfigNode{Type: "notapickle", Extra: map[string]interface{}{"blah": "rockettes"}},
			nil,
			"Node not d",
		},
	}
	for _, v := range data {
		val, err := v.in.Create(p)

		if err != nil && err.Error()[:10] != v.err {
			t.Errorf("expected error %v, got %v", v.err, err.Error()[:10])
			continue
		}
		if !reflect.DeepEqual(v.out, val) && err == nil {
			t.Errorf("expected (%T)%+v, got (%T)%+v", v.out, v.out, val, val)
		}
	}
}
