package skip

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/compose/transporter/function"
	_ "github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

var errorTests = []struct {
	name     string
	expected string
	e        error
}{
	{
		"WrongTypeError",
		"value is of incompatible type, wanted blah, got blah",
		wrongTypeError{"blah", "blah"},
	},
	{
		"UnknownOperatorError",
		"unkown operator, dosomething",
		unknownOperatorError{"dosomething"},
	},
}

func TestErrors(t *testing.T) {
	for _, et := range errorTests {
		if et.e.Error() != et.expected {
			t.Errorf("[%s] wrong Error(), expected %s, got %s", et.name, et.expected, et.e.Error())
		}
	}
}

var initTests = []struct {
	in     map[string]interface{}
	expect *skip
}{
	{
		map[string]interface{}{"field": "test", "operator": "==", "match": 10},
		&skip{Field: "test", Operator: "==", Match: float64(10)},
	},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := function.GetFunction("skip", it.in)
		if err != nil {
			t.Fatalf("unexpected GetFunction() error, %s", err)
		}
		if !reflect.DeepEqual(a, it.expect) {
			t.Errorf("misconfigured Function, expected %+v, got %+v", it.expect, a)
		}
	}
}

var skipTests = []struct {
	name      string
	field     string
	operators []string
	match     interface{}
	data      map[string]interface{}
	err       error
	skipped   bool
}{
	{
		"unknown operator", "type", []string{"="}, "good", map[string]interface{}{"_id": "blah", "type": "good"}, unknownOperatorError{"="}, true,
	},
	{
		"match", "type", []string{"==", "eq", "$eq"}, "good", map[string]interface{}{"_id": "blah", "type": "good"}, nil, false,
	},
	{
		"skipped", "type", []string{"==", "eq", "$eq"}, "bad", map[string]interface{}{"_id": "blah", "type": "good"}, nil, true,
	},
	{
		"match", "type", []string{"=~"}, "good", map[string]interface{}{"_id": "blah", "type": "goodnight"}, nil, false,
	},
	{
		"skipped", "type", []string{"=~"}, "^good", map[string]interface{}{"_id": "blah", "type": "very good"}, nil, true,
	},
	{
		"match", "count", []string{">", "gt", "$gt"}, 10, map[string]interface{}{"_id": "blah", "count": 11}, nil, false,
	},
	{
		"skipped", "count", []string{">", "gt", "$gt"}, 10, map[string]interface{}{"_id": "blah", "count": 10}, nil, true,
	},
	{
		"match", "count", []string{">", "gt", "$gt"}, 10.5, map[string]interface{}{"_id": "blah", "count": 11}, nil, false,
	},
	{
		"skipped", "count", []string{">", "gt", "$gt"}, 10.5, map[string]interface{}{"_id": "blah", "count": 10}, nil, true,
	},
	{
		"match", "count", []string{">=", "gte", "$gte"}, 10, map[string]interface{}{"_id": "blah", "count": 10}, nil, false,
	},
	{
		"match", "count", []string{">=", "gte", "$gte"}, 10, map[string]interface{}{"_id": "blah", "count": 10.5}, nil, false,
	},
	{
		"skipped", "count", []string{">=", "gte", "$gte"}, 10, map[string]interface{}{"_id": "blah", "count": 9.5}, nil, true,
	},
	{
		"match", "count", []string{"<", "lt", "$lt"}, 10, map[string]interface{}{"_id": "blah", "count": 9}, nil, false,
	},
	{
		"skipped", "count", []string{"<", "lt", "$lt"}, 10, map[string]interface{}{"_id": "blah", "count": 10}, nil, true,
	},
	{
		"match", "count", []string{"<=", "lte", "$lte"}, 10, map[string]interface{}{"_id": "blah", "count": 9.9}, nil, false,
	},
	{
		"match", "count", []string{"<=", "lte", "$lte"}, 10, map[string]interface{}{"_id": "blah", "count": 10}, nil, false,
	},
	{
		"skipped", "count", []string{"<=", "lte", "$lte"}, 10, map[string]interface{}{"_id": "blah", "count": 10.1}, nil, true,
	},
	{
		"match", "count", []string{"lte"}, "10", map[string]interface{}{"_id": "blah", "count": 10}, nil, false,
	},
	{
		"match", "count", []string{"lte"}, 10, map[string]interface{}{"_id": "blah", "count": "10"}, nil, false,
	},
	{
		"wrong type", "count", []string{"<="}, "10", map[string]interface{}{"_id": "blah", "count": 10.1}, nil, true,
	},
	{
		"wrong type", "count", []string{"<="}, 10, map[string]interface{}{"_id": "blah", "count": "10.1"}, nil, true,
	},
	{
		"uncovertable string", "count", []string{"<="}, "ten", map[string]interface{}{"_id": "blah", "count": 10.1}, &strconv.NumError{Func: "ParseFloat", Num: "ten", Err: strconv.ErrSyntax}, true,
	},
	{
		"uncovertable string", "count", []string{"<="}, 10, map[string]interface{}{"_id": "blah", "count": "ten"}, &strconv.NumError{Func: "ParseFloat", Num: "ten", Err: strconv.ErrSyntax}, true,
	},
	{
		"wrong type", "count", []string{"<="}, true, map[string]interface{}{"_id": "blah", "count": 10.1}, wrongTypeError{"float64 or int", "bool"}, true,
	},
	{
		"wrong type", "count", []string{"<="}, 10, map[string]interface{}{"_id": "blah", "count": false}, wrongTypeError{"float64 or int", "bool"}, true,
	},
}

func TestApply(t *testing.T) {
	for _, st := range skipTests {
		for _, op := range st.operators {
			skip := &skip{st.field, op, st.match}
			msg, err := skip.Apply(message.From(ops.Insert, "test", st.data))
			if !reflect.DeepEqual(err, st.err) {
				t.Errorf("[%s %s] error mismatch, expected %s, got %s", op, st.name, st.err, err)
			}
			if (msg == nil) != st.skipped {
				t.Errorf("[%s %s] skip mismatch, expected %v, got %v", op, st.name, st.skipped, (msg == nil))
			}
		}
	}
}
