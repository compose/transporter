package rename

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
	expect *rename
}{
	{
		map[string]interface{}{"field_map": map[string]string{"test": "newtest"}},
		&rename{SwapMap: map[string]string{"test": "newtest"}},
	},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := function.GetFunction("rename", it.in)
		if err != nil {
			t.Fatalf("unexpected GetFunction() error, %s", err)
		}
		if !reflect.DeepEqual(a, it.expect) {
			t.Errorf("misconfigured Function, expected %+v, got %+v", it.expect, a)
		}
	}
}

var renameTests = []struct {
	name     string
	fieldMap map[string]string
	in       map[string]interface{}
	out      map[string]interface{}
	err      error
}{
	{
		"single field",
		map[string]string{"type": "expression"},
		map[string]interface{}{"_id": "blah", "type": "good"},
		map[string]interface{}{"_id": "blah", "expression": "good"},
		nil,
	},
	{
		"multiple fields",
		map[string]string{"_id": "id", "name": "n"},
		map[string]interface{}{"_id": "blah", "type": "good", "name": "hello"},
		map[string]interface{}{"id": "blah", "type": "good", "n": "hello"},
		nil,
	},
	{
		"no matched fields",
		map[string]string{"name": "n"},
		map[string]interface{}{"_id": "blah", "type": "good"},
		map[string]interface{}{"_id": "blah", "type": "good"},
		nil,
	},
}

func TestApply(t *testing.T) {
	for _, rt := range renameTests {
		rename := &rename{rt.fieldMap}
		msg, err := rename.Apply(message.From(ops.Insert, "test", rt.in))
		if !reflect.DeepEqual(err, rt.err) {
			t.Errorf("[%s] error mismatch, expected %s, got %s", rt.name, rt.err, err)
		}
		if !reflect.DeepEqual(msg.Data().AsMap(), rt.out) {
			t.Errorf("[%s] wrong message, expected %+v, got %+v", rt.name, rt.out, msg.Data().AsMap())
		}
	}
}
