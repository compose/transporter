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

func NewTestImpl(p *pipe.Pipe, extra ExtraConfig) (Impl, error) {
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

func TestCreateImpl(t *testing.T) {
	Register("testimpl", NewTestImpl)

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
