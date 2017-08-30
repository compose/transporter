package remap

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
	expect *remap
}{
	{
		map[string]interface{}{"ns_map": map[string]string{"test": "newtest"}},
		&remap{SwapMap: map[string]string{"test": "newtest"}},
	},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := function.GetFunction("remap", it.in)
		if err != nil {
			t.Fatalf("unexpected GetFunction() error, %s", err)
		}
		if !reflect.DeepEqual(a, it.expect) {
			t.Errorf("misconfigured Function, expected %+v, got %+v", it.expect, a)
		}
	}
}

var remapTests = []struct {
	name  string
	nsMap map[string]string
	in    map[string]interface{}
	inNs  string
	outNs string
	err   error
}{
	{
		"single field",
		map[string]string{"foo": "bar"},
		map[string]interface{}{"_id": "blah", "type": "good"},
		"foo",
		"bar",
		nil,
	},
	{
		"multiple fields",
		map[string]string{"foo": "bar", "baz": "boo"},
		map[string]interface{}{"_id": "blah", "type": "good", "name": "hello"},
		"baz",
		"boo",
		nil,
	},
	{
		"no matched fields",
		map[string]string{"blah": "hey"},
		map[string]interface{}{"_id": "blah", "type": "good"},
		"foo",
		"foo",
		nil,
	},
}

func TestApply(t *testing.T) {
	for _, rt := range remapTests {
		remap := &remap{rt.nsMap}
		msg, err := remap.Apply(message.From(ops.Insert, rt.inNs, rt.in))
		if !reflect.DeepEqual(err, rt.err) {
			t.Errorf("[%s] error mismatch, expected %s, got %s", rt.name, rt.err, err)
		}
		if !reflect.DeepEqual(msg.Namespace(), rt.outNs) {
			t.Errorf("[%s] wrong message, expected %+v, got %+v", rt.name, rt.outNs, msg.Namespace())
		}
	}
}
