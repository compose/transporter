package pick

import (
	"reflect"
	"testing"

	"github.com/compose/transporter/function"
	_ "github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

var initTests = []struct {
	in     map[string]interface{}
	expect *picker
}{
	{map[string]interface{}{"fields": []string{"test"}}, &picker{Fields: []string{"test"}}},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := function.GetFunction("pick", it.in)
		if err != nil {
			t.Fatalf("unexpected GetFunction() error, %s", err)
		}
		if !reflect.DeepEqual(a, it.expect) {
			t.Errorf("misconfigured Function, expected %+v, got %+v", it.expect, a)
		}
	}
}

var pickTests = []struct {
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
		map[string]interface{}{"type": "good"},
		nil,
	},
	{
		"multiple fields",
		[]string{"_id", "name"},
		map[string]interface{}{"_id": "blah", "type": "good", "name": "hello"},
		map[string]interface{}{"_id": "blah", "name": "hello"},
		nil,
	},
	{
		"no matched fields",
		[]string{"name"},
		map[string]interface{}{"_id": "blah", "type": "good"},
		map[string]interface{}{},
		nil,
	},
}

func TestApply(t *testing.T) {
	for _, pt := range pickTests {
		pick := &picker{pt.fields}
		msg, err := pick.Apply(message.From(ops.Insert, "test", pt.in))
		if !reflect.DeepEqual(err, pt.err) {
			t.Errorf("[%s] error mismatch, expected %s, got %s", pt.name, pt.err, err)
		}
		if !reflect.DeepEqual(msg.Data().AsMap(), pt.out) {
			t.Errorf("[%s] wrong message, expected %+v, got %+v", pt.name, pt.out, msg.Data().AsMap())
		}
	}
}
