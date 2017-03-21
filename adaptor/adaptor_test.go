package adaptor_test

import (
	"reflect"
	"testing"

	"github.com/compose/transporter/adaptor"
	_ "github.com/compose/transporter/log"
)

func init() {
	adaptor.Add("mock", func() adaptor.Adaptor { return &adaptor.Mock{} })
	adaptor.Add("unsupported", func() adaptor.Adaptor { return &adaptor.UnsupportedMock{} })
}

func TestMocks(t *testing.T) {
	m, err := adaptor.GetAdaptor("mock", map[string]interface{}{"uri": "uri", "namespace": "namespace"})
	if err != nil {
		t.Errorf("unexpected GetV2() error, %s", err)
	}
	if _, err := m.Client(); err != nil {
		t.Errorf("unexpected Client() error, %s", err)
	}
	if _, err := m.Reader(); err != nil {
		t.Errorf("unexpected Reader() error, %s", err)
	}
	if _, err := m.Writer(nil, nil); err != nil {
		t.Errorf("unexpected Writer() error, %s", err)
	}

	_, err = adaptor.GetAdaptor("notfound", map[string]interface{}{})
	aerr := adaptor.ErrNotFound{Name: "notfound"}
	if !reflect.DeepEqual(err.Error(), aerr.Error()) {
		t.Errorf("err mismatch, expected %+v, got %+v", aerr, err)
	}
}

func TestUnsupportedMock(t *testing.T) {
	aName := "unsupported"
	m, err := adaptor.GetAdaptor(aName, map[string]interface{}{})
	if err != nil {
		t.Errorf("unexpected GetV2() error, %s", err)
	}
	uerr := adaptor.ErrFuncNotSupported{Name: aName, Func: "Client()"}
	if _, err := m.Client(); !reflect.DeepEqual(err.Error(), uerr.Error()) {
		t.Errorf("wrong Client() error, expected %s, got %s", uerr, err)
	}
	uerr = adaptor.ErrFuncNotSupported{Name: aName, Func: "Reader()"}
	if _, err := m.Reader(); !reflect.DeepEqual(err.Error(), uerr.Error()) {
		t.Errorf("wrong Reader() error, expected %s, got %s", uerr, err)
	}
	uerr = adaptor.ErrFuncNotSupported{Name: aName, Func: "Writer()"}
	if _, err := m.Writer(nil, nil); !reflect.DeepEqual(err.Error(), uerr.Error()) {
		t.Errorf("wrong Writer() error, expected %s, got %s", uerr, err)
	}
}

func TestRegisteredAdaptors(t *testing.T) {
	all := adaptor.RegisteredAdaptors()
	if len(all) != 2 {
		t.Error("wrong number of registered adaptors, expected 2, got %d", len(all))
	}
}

func TestAdaptors(t *testing.T) {
	all := adaptor.Adaptors()
	if len(all) != 2 {
		t.Error("wrong number of registered adaptors, expected 2, got %d", len(all))
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
