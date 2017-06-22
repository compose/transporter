package adaptor_test

import (
	"reflect"
	"testing"

	"github.com/compose/transporter/adaptor"
	_ "github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

func init() {
	adaptor.Add("mock", func() adaptor.Adaptor { return &adaptor.Mock{} })
	adaptor.Add("unsupported", func() adaptor.Adaptor { return &adaptor.UnsupportedMock{} })
	adaptor.Add("clienterr", func() adaptor.Adaptor { return &adaptor.MockClientErr{} })
	adaptor.Add("writererr", func() adaptor.Adaptor { return &adaptor.MockWriterErr{} })
}

var (
	mockTests = []struct {
		name       string
		conf       adaptor.Config
		adaptorErr error
		clientErr  error
		readerErr  error
		writerErr  error
	}{
		{"mock", map[string]interface{}{"uri": "uri"}, nil, nil, nil, nil},
		{"clienterr", map[string]interface{}{"uri": "uri"}, nil, nil, nil, nil},
		{"writererr", map[string]interface{}{"uri": "uri"}, nil, nil, nil, nil},
		{"notfound", map[string]interface{}{}, adaptor.ErrNotFound{Name: "notfound"}, nil, nil, nil},
		{
			"unsupported",
			map[string]interface{}{},
			nil,
			adaptor.ErrFuncNotSupported{Name: "unsupported", Func: "Client()"},
			adaptor.ErrFuncNotSupported{Name: "unsupported", Func: "Reader()"},
			adaptor.ErrFuncNotSupported{Name: "unsupported", Func: "Writer()"},
		},
	}
)

func TestMocks(t *testing.T) {
	for _, mt := range mockTests {
		m, err := adaptor.GetAdaptor(mt.name, mt.conf)
		if !reflect.DeepEqual(err, mt.adaptorErr) {
			t.Errorf("[%s] wrong GetAdaptor() error, expected %s, got %s", mt.name, mt.adaptorErr, err)
		}
		if err == nil {
			if _, err := m.Client(); !reflect.DeepEqual(err, mt.clientErr) {
				t.Errorf("[%s] wrong Client() error, expected %s, got %s", mt.name, mt.clientErr, err)
			}
			if _, err := m.Reader(); !reflect.DeepEqual(err, mt.readerErr) {
				t.Errorf("[%s] wrong Reader() error, expected %s, got %s", mt.name, mt.readerErr, err)
			}
			if _, err := m.Writer(nil, nil); !reflect.DeepEqual(err, mt.writerErr) {
				t.Errorf("[%s] wrong Writer() error, expected %s, got %s", mt.name, mt.writerErr, err)
			}
		}
	}
}

func TestRegisteredAdaptors(t *testing.T) {
	all := adaptor.RegisteredAdaptors()
	if len(all) != 4 {
		t.Errorf("wrong number of registered adaptors, expected 4, got %d", len(all))
	}
}

func TestAdaptors(t *testing.T) {
	all := adaptor.Adaptors()
	if len(all) != 4 {
		t.Errorf("wrong number of registered adaptors, expected 4, got %d", len(all))
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

func TestMockConfirms(t *testing.T) {
	confirms, cleanup := adaptor.MockConfirmWrites()
	defer adaptor.VerifyWriteConfirmed(cleanup, t)
	m, err := adaptor.GetAdaptor("mock", map[string]interface{}{"uri": "uri"})
	if err != nil {
		t.Fatalf("unexpected GetAdaptor error, %s", err)
	}
	w, err := m.Writer(nil, nil)
	if err != nil {
		t.Fatalf("unexpected Writer error, %s", err)
	}
	msg := message.From(ops.Insert, "test", map[string]interface{}{"id": 0, "test": "hello world"})
	msg = message.WithConfirms(confirms, msg)
	if _, err = w.Write(msg)(nil); err != nil {
		t.Errorf("unexpected Writer error, %s", err)
	}
}
