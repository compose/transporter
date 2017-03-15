package skip

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/compose/transporter/adaptor"
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
		WrongTypeError{"blah", "blah"},
	},
	{
		"UnknownOperatorError",
		"unkown operator, dosomething",
		UnknownOperatorError{"dosomething"},
	},
}

func TestErrors(t *testing.T) {
	for _, et := range errorTests {
		if et.e.Error() != et.expected {
			t.Errorf("[%s] wrong Error(), expected %s, got %s", et.name, et.expected, et.e.Error())
		}
	}
}

var initTests = []map[string]interface{}{
	{"field": "test", "operator": "==", "match": 10},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := adaptor.GetAdaptor("skip", it)
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
		"unknown operator", "type", []string{"="}, "good", map[string]interface{}{"_id": "blah", "type": "good"}, UnknownOperatorError{"="}, true,
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
		"uncovertable string", "count", []string{"<="}, "ten", map[string]interface{}{"_id": "blah", "count": 10.1}, &strconv.NumError{"ParseFloat", "ten", strconv.ErrSyntax}, true,
	},
	{
		"uncovertable string", "count", []string{"<="}, 10, map[string]interface{}{"_id": "blah", "count": "ten"}, &strconv.NumError{"ParseFloat", "ten", strconv.ErrSyntax}, true,
	},
	{
		"wrong type", "count", []string{"<="}, true, map[string]interface{}{"_id": "blah", "count": 10.1}, WrongTypeError{"float64 or int", "bool"}, true,
	},
	{
		"wrong type", "count", []string{"<="}, 10, map[string]interface{}{"_id": "blah", "count": false}, WrongTypeError{"float64 or int", "bool"}, true,
	},
}

func TestSkip(t *testing.T) {
	for _, st := range skipTests {
		for _, op := range st.operators {
			skip := &Skip{st.field, op, st.match}
			msg, err := skip.Write(message.From(ops.Insert, "test", st.data))(nil)
			if !reflect.DeepEqual(err, st.err) {
				t.Errorf("[%s %s] error mismatch, expected %s, got %s", op, st.name, st.err, err)
			}
			if (msg == nil) != st.skipped {
				t.Errorf("[%s %s] skip mismatch, expected %v, got %v", op, st.name, st.skipped, (msg == nil))
			}
		}
	}
}
