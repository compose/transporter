package impl

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/pipe"
)

// var anError = errors.New("this is an error")

// a random type that implements the Impl interface
type TestImpl struct {
	value string
}

func NewTestImpl(p *pipe.Pipe, extra map[string]interface{}) (*TestImpl, error) {
	val, ok := extra["value"]
	if !ok {
		return nil, errors.New("this is an error")
	}
	return &TestImpl{value: val.(string)}, nil
}

func (s *TestImpl) Start() error {
	return nil
}

func (s *TestImpl) Stop() error {
	return nil
}

func (s *TestImpl) Listen() error {
	return nil
}

type BadImpl struct {
	value string
}

func NewBadImpl(p *pipe.Pipe, extra map[string]interface{}) (*BadImpl, error) {
	val, ok := extra["value"]
	if !ok {
		return nil, errors.New("this is an error")
	}
	return &BadImpl{value: val.(string)}, nil
}

func TestCreateImpl(t *testing.T) {
	Registry["testimpl"] = NewTestImpl
	Registry["badimpl"] = NewBadImpl

	data := []struct {
		kind  string
		extra map[string]interface{}
		out   *TestImpl
		err   string
	}{
		{
			"testimpl",
			map[string]interface{}{"value": "rockettes"},
			&TestImpl{value: "rockettes"},
			"",
		},
		{
			"testimpl",
			map[string]interface{}{"blah": "rockettes"},
			&TestImpl{},
			"this is an error",
		},
		{
			"badimpl",
			map[string]interface{}{"value": "rockettes"},
			nil,
			"cannot create node: interface conversion: *impl.BadImpl is not impl.Impl: missing method Listen",
		},
		{
			"notasource",
			map[string]interface{}{"blah": "rockettes"},
			nil,
			"Impl not found in registry",
		},
	}
	for _, v := range data {
		impl, err := CreateImpl(v.kind, v.extra, pipe.NewPipe(nil, "some name", 1*time.Second))

		if err != nil && err.Error() != v.err {
			t.Errorf("\nexpected error: %v\ngot error: %v\n", v.err, err.Error())
			t.FailNow()
		}
		if !reflect.DeepEqual(v.out, impl) && err == nil {
			t.Errorf("expected:\n%+v\ngot:\n%+v\n", v.out, impl)
		}
	}
}
