package adaptor

import (
	"errors"
	"reflect"
	"testing"

	"git.compose.io/compose/transporter/pkg/pipe"
)

// a random type that adaptor implements the adaptor interface
type TestAdaptor struct {
	value string
}

var errTest = errors.New("this is an error")

func init() {
	Add("testadaptor", func(p *pipe.Pipe, path string, extra Config) (Adaptor, error) {
		val, ok := extra["value"]
		if !ok {
			return nil, errTest
		}
		return &TestAdaptor{value: val.(string)}, nil
	})
}

func (s *TestAdaptor) Description() string {
	return "this is a test adaptor"
}

func (s *TestAdaptor) SampleConfig() string {
	return ""
}

func (s *TestAdaptor) Connect() error {
	return nil
}

func (s *TestAdaptor) Start() error {
	return nil
}

func (s *TestAdaptor) Stop() error {
	return nil
}

func (s *TestAdaptor) Listen() error {
	return nil
}

func TestCreateAdaptor(t *testing.T) {
	data := []struct {
		kind  string
		extra Config
		out   *TestAdaptor
		err   string
	}{
		{
			"testadaptor",
			Config{"value": "rockettes"},
			&TestAdaptor{value: "rockettes"},
			"",
		},
		{
			"testadaptor",
			Config{"blah": "rockettes"},
			&TestAdaptor{},
			errTest.Error(),
		},
		{
			"notasource",
			Config{"blah": "rockettes"},
			nil,
			"adaptor 'notasource' not found in registry",
		},
	}
	for _, v := range data {
		adaptor, err := CreateAdaptor(v.kind, "a/b/c", v.extra, pipe.NewPipe(nil, "some name"))

		if err != nil && err.Error() != v.err {
			t.Errorf("\nexpected error: `%v`\ngot error: `%v`\n", v.err, err.Error())
			t.FailNow()
		}
		if !reflect.DeepEqual(v.out, adaptor) && err == nil {
			t.Errorf("expected:\n%+v\ngot:\n%+v\n", v.out, adaptor)
		}
	}
}
