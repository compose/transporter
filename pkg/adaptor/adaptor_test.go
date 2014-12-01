package adaptor

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/pipe"
)

// var anError = errors.New("this is an error")

// a random type that adaptorements the adaptor interface
type Testadaptor struct {
	value string
}

func NewTestadaptor(p *pipe.Pipe, extra Config) (StopStartListener, error) {
	val, ok := extra["value"]
	if !ok {
		return nil, errors.New("this is an error")
	}
	return &Testadaptor{value: val.(string)}, nil
}

func (s *Testadaptor) Start() error {
	return nil
}

func (s *Testadaptor) Stop() error {
	return nil
}

func (s *Testadaptor) Listen() error {
	return nil
}

func TestCreateadaptor(t *testing.T) {
	Register("testadaptor", NewTestadaptor)

	data := []struct {
		kind  string
		extra map[string]interface{}
		out   *Testadaptor
		err   string
	}{
		{
			"testadaptor",
			map[string]interface{}{"value": "rockettes"},
			&Testadaptor{value: "rockettes"},
			"",
		},
		{
			"testadaptor",
			map[string]interface{}{"blah": "rockettes"},
			&Testadaptor{},
			"this is an error",
		},
		{
			"notasource",
			map[string]interface{}{"blah": "rockettes"},
			nil,
			"adaptor not found in registry",
		},
	}
	for _, v := range data {
		adaptor, err := Createadaptor(v.kind, v.extra, pipe.NewPipe(nil, "some name", 1*time.Second))

		if err != nil && err.Error() != v.err {
			t.Errorf("\nexpected error: %v\ngot error: %v\n", v.err, err.Error())
			t.FailNow()
		}
		if !reflect.DeepEqual(v.out, adaptor) && err == nil {
			t.Errorf("expected:\n%+v\ngot:\n%+v\n", v.out, adaptor)
		}
	}
}
