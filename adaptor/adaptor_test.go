package adaptor_test

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"testing"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/pipe"
)

// a random type that adaptor implements the adaptor interface
type TestAdaptor struct {
	value string
}

type TestConf struct {
}

var errTest = errors.New("this is an error")

func init() {
	adaptor.Add("testadaptor", func(p *pipe.Pipe, path string, extra adaptor.Config) (adaptor.Adaptor, error) {
		val, ok := extra["value"]
		if !ok {
			return nil, errTest
		}
		var conf TestConf
		if err := extra.Construct(&conf); err != nil {
			return nil, adaptor.Error{adaptor.CRITICAL, path, fmt.Sprintf("bad config (%s)", err.Error()), nil}
		}
		return &TestAdaptor{value: val.(string)}, nil
	})

	adaptor.Add("connectfail", func(p *pipe.Pipe, path string, extra adaptor.Config) (adaptor.Adaptor, error) {
		return &ConnectFailAdaptor{}, nil
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

type ConnectFailAdaptor struct {
}

func (a *ConnectFailAdaptor) Connect() error {
	return client.ConnectError{Reason: "I am a test"}
}

func (a *ConnectFailAdaptor) Start() error {
	return nil
}

func (a *ConnectFailAdaptor) Stop() error {
	return nil
}

func (a *ConnectFailAdaptor) Listen() error {
	return nil
}

var data = []struct {
	kind  string
	extra adaptor.Config
	out   adaptor.Adaptor
	err   error
}{
	{"testadaptor", adaptor.Config{"value": "rockettes"}, &TestAdaptor{value: "rockettes"}, nil},
	{"testadaptor", adaptor.Config{"blah": "rockettes"}, &TestAdaptor{}, errTest},
	{"notasource", adaptor.Config{"blah": "rockettes"}, nil, adaptor.ErrNotFound{"notasource"}},
	{"connectfail", adaptor.Config{}, &ConnectFailAdaptor{}, client.ConnectError{Reason: "I am a test"}},
}

func TestCreateAdaptor(t *testing.T) {
	for _, v := range data {
		adaptor, err := adaptor.CreateAdaptor(v.kind, "a/b/c", v.extra, pipe.NewPipe(nil, "some name"))

		if !reflect.DeepEqual(err, v.err) {
			t.Errorf("[%s] wrong error, expected: %v got: %v", v.kind, v.err, err)
		}

		if v.err != nil {
			if !reflect.DeepEqual(err.Error(), v.err.Error()) {
				t.Errorf("[%s] wrong Error(), expected: %s got: %s", v.kind, v.err.Error(), err.Error())
			}
		}

		if !reflect.DeepEqual(v.out, adaptor) && err == nil {
			t.Errorf("[%s] wrong adaptor, expected: %+v got: %+v", v.kind, v.out, adaptor)
		}
	}
}

var configTests = []struct {
	cfg      adaptor.Config
	key      string
	expected string
}{
	{adaptor.Config{"hello": "world"}, "hello", "world"},
	{adaptor.Config{"hello": "world"}, "goodbye", ""},
	{adaptor.Config{"key": 1}, "key", ""},
}

func TestConfig(t *testing.T) {
	for _, ct := range configTests {
		val := ct.cfg.GetString(ct.key)
		if !reflect.DeepEqual(val, ct.expected) {
			t.Errorf("wrong string returned for %s, expected %s, got %s", ct.key, ct.expected, val)
		}
	}
}

var compileNamespaceTests = []struct {
	name    string
	cfg     adaptor.Config
	partOne string
	r       *regexp.Regexp
	err     error
}{
	{
		"simple ns",
		adaptor.Config{"namespace": "a.b"},
		"a",
		regexp.MustCompile("b"),
		nil,
	},
	{
		"simple regexp ns",
		adaptor.Config{"namespace": "a..*"},
		"a",
		regexp.MustCompile(".*"),
		nil,
	},
	{
		"simple regexp ns with /",
		adaptor.Config{"namespace": "a./.*/"},
		"a",
		regexp.MustCompile(".*"),
		nil,
	},
	{
		"malformed regexp",
		adaptor.Config{"namespace": "a"},
		"",
		nil,
		adaptor.ErrNamespaceMalformed,
	},
}

func TestCompileNamespace(t *testing.T) {
	for _, ct := range compileNamespaceTests {
		out, r, err := ct.cfg.CompileNamespace()
		if !reflect.DeepEqual(out, ct.partOne) {
			t.Errorf("[%s] wrong value returned, expected %s, got %s", ct.name, ct.partOne, out)
		}
		if !reflect.DeepEqual(r, ct.r) {
			t.Errorf("[%s] wrong regexp returned, expected %+v, got %+v", ct.name, ct.r, r)
		}
		if err != ct.err {
			t.Errorf("[%s] wrong error returned, expected %+v, got %+v", ct.name, ct.err, err)
		}
	}
}
