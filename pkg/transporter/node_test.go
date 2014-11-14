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
type SourceImpl struct {
	value string
}

func NewSourceImpl(p pipe.Pipe, extra map[string]interface{}) (*SourceImpl, error) {
	val, ok := extra["value"]
	if !ok {
		return nil, anError
	}
	return &SourceImpl{value: val.(string)}, nil
}

func (s *SourceImpl) Stop() error {
	return nil
}

func (s *SourceImpl) Start() error {
	return nil
}

// a random type that implements the node interface
type NodeImpl struct {
	value string
}

func NewNodeImpl(p pipe.Pipe, extra map[string]interface{}) (*NodeImpl, error) {
	val, ok := extra["value"]
	if !ok {
		return nil, anError
	}
	return &NodeImpl{value: val.(string)}, nil
}

func (s *NodeImpl) Stop() error {
	return nil
}

func (s *NodeImpl) Listen() error {
	return nil
}

func TestConfigNodeCreateSource(t *testing.T) {
	p := pipe.NewSourcePipe("name", 1*time.Second)

	SourceRegistry = map[string]interface{}{
		"source":     NewSourceImpl,
		"notasource": NewImpl,
	}

	data := []struct {
		in  ConfigNode
		out *SourceImpl
		err error
	}{
		{
			ConfigNode{Type: "source", Extra: map[string]interface{}{"value": "rockettes"}},
			&SourceImpl{value: "rockettes"},
			nil,
		},
		{
			ConfigNode{Type: "source", Extra: map[string]interface{}{"blah": "rockettes"}},
			nil,
			anError,
		},
		{
			ConfigNode{Type: "notasource", Extra: map[string]interface{}{"value": "rockettes"}},
			nil,
			NoNodeError,
		},
		{
			ConfigNode{Type: "notasource", Extra: map[string]interface{}{"blah": "rockettes"}},
			nil,
			anError,
		},
		{
			ConfigNode{Type: "notaevenlisted", Extra: map[string]interface{}{"blah": "rockettes"}},
			nil,
			MissingNodeError,
		},
	}
	for _, v := range data {
		val, err := v.in.CreateSource(p)

		if err != v.err {
			t.Errorf("expected error %v, got %v", v.err, err)
			continue
		}
		if !reflect.DeepEqual(v.out, val) && err == nil {
			t.Errorf("expected (%T)%+v, got (%T)%+v", v.out, v.out, val, val)
		}
	}
}

func TestConfigNodeCreate(t *testing.T) {
	p := pipe.NewSourcePipe("name", 1*time.Second)

	NodeRegistry = map[string]interface{}{
		"node":       NewNodeImpl,
		"notasource": NewImpl,
	}

	data := []struct {
		in  ConfigNode
		out *NodeImpl
		err error
	}{
		{
			ConfigNode{Type: "node", Extra: map[string]interface{}{"value": "rockettes"}},
			&NodeImpl{value: "rockettes"},
			nil,
		},
		{
			ConfigNode{Type: "node", Extra: map[string]interface{}{"blah": "rockettes"}},
			nil,
			anError,
		},
		{
			ConfigNode{Type: "notasource", Extra: map[string]interface{}{"value": "rockettes"}},
			nil,
			NoNodeError,
		},
		{
			ConfigNode{Type: "notasource", Extra: map[string]interface{}{"blah": "rockettes"}},
			nil,
			anError,
		},
		{
			ConfigNode{Type: "notapickle", Extra: map[string]interface{}{"blah": "rockettes"}},
			nil,
			MissingNodeError,
		},
	}
	for _, v := range data {
		val, err := v.in.Create(p)

		if err != v.err {
			t.Errorf("expected error %v, got %v", v.err, err)
			continue
		}
		if !reflect.DeepEqual(v.out, val) && err == nil {
			t.Errorf("expected (%T)%+v, got (%T)%+v", v.out, v.out, val, val)
		}
	}
}
