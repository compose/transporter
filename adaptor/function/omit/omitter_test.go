package omit

import (
	"reflect"
	"testing"

	"github.com/compose/transporter/adaptor"
	_ "github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

var initTests = []map[string]interface{}{
	{"fields": []string{"test"}},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := adaptor.GetAdaptor("omit", it)
		if err != nil {
			t.Fatalf("unexpected GetAdaptor() error, %s", err)
		}
		if _, err := a.Client(); err != nil {
			t.Errorf("unexpected Client() error, %s", err)
		}
		rerr := adaptor.ErrFuncNotSupported{Name: "transformer", Func: "Reader()"}
		if _, err := a.Reader(); err != rerr {
			t.Errorf("wrong Reader() error, expected %s, got %s", rerr, err)
		}
		if _, err := a.Writer(nil, nil); err != nil {
			t.Errorf("unexpected Writer() error, %s", err)
		}
	}
}

var omitTests = []struct {
	name   string
	fields []string
	in     map[string]interface{}
	out    map[string]interface{}
	err    error
}{
	{
		"single field",
		[]string{"type"},
		map[string]interface{}{"_id": "blah", "type": "good"},
		map[string]interface{}{"_id": "blah"},
		nil,
	},
	{
		"multiple fields",
		[]string{"type", "name"},
		map[string]interface{}{"_id": "blah", "type": "good", "name": "hello"},
		map[string]interface{}{"_id": "blah"},
		nil,
	},
	{
		"no matched fields",
		[]string{"name"},
		map[string]interface{}{"_id": "blah", "type": "good"},
		map[string]interface{}{"_id": "blah", "type": "good"},
		nil,
	},
}

func TestOmit(t *testing.T) {
	for _, ot := range omitTests {
		omit := &Omitter{ot.fields}
		msg, err := omit.Write(message.From(ops.Insert, "test", ot.in))(nil)
		if !reflect.DeepEqual(err, ot.err) {
			t.Errorf("[%s] error mismatch, expected %s, got %s", ot.name, ot.err, err)
		}
		if !reflect.DeepEqual(msg.Data().AsMap(), ot.out) {
			t.Errorf("[%s] wrong message, expected %+v, got %+v", ot.name, ot.out, msg.Data().AsMap())
		}
	}
}
