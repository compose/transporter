package adaptor

import (
	"errors"
	"reflect"
	"testing"

	"github.com/compose/transporter/pkg/pipe"
)

// a random type that adaptorements the adaptor interface
type Testadaptor struct {
	value string
}

func NewTestadaptor(p *pipe.Pipe, path string, extra Config) (StopStartListener, error) {
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
	Register("testadaptor", "description", NewTestadaptor, struct{}{})

	data := []struct {
		kind  string
		extra Config
		out   *Testadaptor
		err   string
	}{
		{
			"testadaptor",
			Config{"value": "rockettes"},
			&Testadaptor{value: "rockettes"},
			"",
		},
		{
			"testadaptor",
			Config{"blah": "rockettes"},
			&Testadaptor{},
			"cannot create testadaptor adaptor (a/b/c). this is an error",
		},
		{
			"notasource",
			Config{"blah": "rockettes"},
			nil,
			"adaptor 'notasource' not found in registry",
		},
	}
	for _, v := range data {
		adaptor, err := Createadaptor(v.kind, "a/b/c", v.extra, pipe.NewPipe(nil, "some name"))

		if err != nil && err.Error() != v.err {
			t.Errorf("\nexpected error: `%v`\ngot error: `%v`\n", v.err, err.Error())
			t.FailNow()
		}
		if !reflect.DeepEqual(v.out, adaptor) && err == nil {
			t.Errorf("expected:\n%+v\ngot:\n%+v\n", v.out, adaptor)
		}
	}
}
