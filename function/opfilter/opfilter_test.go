package opfilter

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
	expect *Opfilter
}{
	{
		map[string]interface{}{"whitelist": []string{"insert"}},
		&Opfilter{
			Whitelist: []string{ops.Insert.String()},
		},
	},
	{
		map[string]interface{}{"blacklist": []string{"delete"}},
		&Opfilter{
			Blacklist: []string{ops.Delete.String()},
		},
	},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := function.GetFunction("opfilter", it.in)
		if err != nil {
			t.Fatalf("unexpected GetFunction() error, %s", err)
		}
		if !reflect.DeepEqual(a, it.expect) {
			t.Errorf("misconfigured Function, expected %+v, got %+v", it.expect, a)
		}
	}
}

var opfilterTests = []struct {
	name      string
	whitelist []string
	blacklist []string
	in        map[string]interface{}
	inOp      ops.Op
	out       map[string]interface{}
	err       error
}{
	{
		"whitelisted",
		[]string{ops.Insert.String()},
		[]string{},
		map[string]interface{}{"_id": "blah", "type": "good"},
		ops.Insert,
		map[string]interface{}{"_id": "blah", "type": "good"},
		nil,
	},
	{
		"not whitelisted",
		[]string{ops.Insert.String(), ops.Update.String()},
		[]string{},
		map[string]interface{}{"_id": "blah", "type": "good"},
		ops.Delete,
		nil,
		nil,
	},
	{
		"blacklisted",
		[]string{},
		[]string{ops.Delete.String()},
		map[string]interface{}{"_id": "blah", "type": "good"},
		ops.Delete,
		nil,
		nil,
	},
	{
		"not blacklisted",
		[]string{},
		[]string{ops.Delete.String()},
		map[string]interface{}{"_id": "blah", "type": "good"},
		ops.Insert,
		map[string]interface{}{"_id": "blah", "type": "good"},
		nil,
	},
}

func TestApply(t *testing.T) {
	for _, ot := range opfilterTests {
		opfilter := &Opfilter{
			Whitelist: ot.whitelist,
			Blacklist: ot.blacklist,
		}
		msg, err := opfilter.Apply(message.From(ot.inOp, "test", ot.in))
		if !reflect.DeepEqual(err, ot.err) {
			t.Errorf("[%s] error mismatch, expected %s, got %s", ot.name, ot.err, err)
		}
		if ot.out == nil && msg != nil {
			t.Errorf("[%s] expected msg to be nil but wasn't", ot.name)
		} else if ot.out != nil && msg == nil {
			t.Errorf("[%s] expected msg to NOT be nil but was", ot.name)
		} else if ot.out != nil && !reflect.DeepEqual(map[string]interface{}(msg.Data()), ot.out) {
			t.Errorf("[%s] wrong message, expected %+v, got %+v", ot.name, ot.out, msg.Data().AsMap())
		}
	}
}
