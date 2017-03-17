package omit

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
	expect *Omitter
}{
	{map[string]interface{}{"fields": []string{"test"}}, &Omitter{Fields: []string{"test"}}},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := function.GetFunction("omit", it.in)
		if err != nil {
			t.Fatalf("unexpected GetFunction() error, %s", err)
		}
		if !reflect.DeepEqual(a, it.expect) {
			t.Errorf("misconfigured Function, expected %+v, got %+v", it.expect, a)
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

func TestApply(t *testing.T) {
	for _, ot := range omitTests {
		omit := &Omitter{ot.fields}
		msg, err := omit.Apply(message.From(ops.Insert, "test", ot.in))
		if !reflect.DeepEqual(err, ot.err) {
			t.Errorf("[%s] error mismatch, expected %s, got %s", ot.name, ot.err, err)
		}
		if !reflect.DeepEqual(msg.Data().AsMap(), ot.out) {
			t.Errorf("[%s] wrong message, expected %+v, got %+v", ot.name, ot.out, msg.Data().AsMap())
		}
	}
}
