package gojajs

import (
	"reflect"
	"testing"

	"github.com/compose/transporter/function"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
	"gopkg.in/mgo.v2/bson"
)

var initTests = []struct {
	in     map[string]interface{}
	expect *Goja
}{
	{map[string]interface{}{"filename": "testdata/transformer.js"}, &Goja{Filename: "testdata/transformer.js"}},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := function.GetFunction("goja", it.in)
		if err != nil {
			t.Fatalf("unexpected GetFunction() error, %s", err)
		}
		if !reflect.DeepEqual(a, it.expect) {
			t.Errorf("misconfigured Function, expected %+v, got %+v", it.expect, a)
		}
	}
}

var (
	bsonID1    = bson.NewObjectId()
	bsonID2    = bson.ObjectIdHex("54a4420502a14b9641000001")
	writeTests = []struct {
		name string
		fn   string
		in   message.Msg
		out  message.Msg
		err  error
	}{
		{
			"just pass through",
			"testdata/transformer.js",
			message.From(ops.Insert, "collection", data.Data{"id": "id1", "name": "nick"}),
			message.From(ops.Insert, "collection", data.Data{"id": "id1", "name": "nick"}),
			nil,
		},
		{
			"delete the 'name' property",
			"testdata/delete_name.js",
			message.From(ops.Insert, "collection", data.Data{"id": "id2", "name": "nick"}),
			message.From(ops.Insert, "collection", data.Data{"id": "id2"}),
			nil,
		},
		{
			"delete's should be processed the same",
			"testdata/delete_name.js",
			message.From(ops.Delete, "collection", data.Data{"id": "id2", "name": "nick"}),
			message.From(ops.Delete, "collection", data.Data{"id": "id2"}),
			nil,
		},
		{
			"bson should marshal and unmarshal properly",
			"testdata/transformer.js",
			message.From(ops.Insert, "collection", data.Data{"id": bsonID1, "name": "nick"}),
			message.From(ops.Insert, "collection", data.Data{"id": bsonID1, "name": "nick"}),
			nil,
		},
		{
			"we should be able to change the bson",
			"testdata/change_bson.js",
			message.From(ops.Insert, "collection", data.Data{"id": bsonID1, "name": "nick"}),
			message.From(ops.Insert, "collection", data.Data{"id": bsonID2, "name": "nick"}),
			nil,
		},
		{
			"we should be able to skip a message",
			"testdata/skip.js",
			message.From(ops.Insert, "collection", data.Data{"id": bsonID1, "name": "nick"}),
			nil,
			nil,
		},
		{
			"we should be able to change the namespace",
			"testdata/change_ns.js",
			message.From(ops.Insert, "collection", data.Data{"id": bsonID1, "name": "nick"}),
			message.From(ops.Insert, "table", data.Data{"id": bsonID1, "name": "nick"}),
			nil,
		}, {
			"we should be able to add an object to the bson",
			"testdata/add_data.js",
			message.From(ops.Insert, "collection", data.Data{"name": "nick"}),
			message.From(ops.Insert, "collection", data.Data{"name": "nick", "added": bson.M{"name": "batman", "villain": "joker"}}),
			nil,
		},
		{
			"Invalid data returned",
			"testdata/invalid_data.js",
			message.From(ops.Insert, "collection", data.Data{"id": "id1", "name": "nick"}),
			nil,
			ErrInvalidMessageType,
		},
		{
			"empty filename",
			"",
			message.From(ops.Insert, "collection", data.Data{"id": "id1", "name": "nick"}),
			nil,
			ErrEmptyFilename,
		},
	}
)

func TestApply(t *testing.T) {
	for _, v := range writeTests {
		g := Goja{Filename: v.fn}
		msg, err := g.Apply(v.in)
		if err != v.err {
			t.Errorf("[%s] wrong error, expected: %+v, got; %v", v.name, v.err, err)
		}
		if !isEqual(msg, v.out) {
			t.Errorf("[%s] expected:\n(%T) %+v\ngot:\n(%T) %+v with error (%v)\n", v.name, v.out, v.out, msg, msg, err)
		}
	}
}

func isEqual(m1 message.Msg, m2 message.Msg) bool {
	if m1 == nil && m2 != nil {
		return false
	}
	if m1 != nil && m2 == nil {
		return false
	}
	if m1 == nil && m2 == nil {
		return true
	}
	if m1.ID() != m2.ID() {
		return false
	}
	if m1.Namespace() != m2.Namespace() {
		return false
	}
	if m1.OP() != m2.OP() {
		return false
	}
	if m1.Timestamp() != m2.Timestamp() {
		return false
	}
	if reflect.TypeOf(m1.Data()) != reflect.TypeOf(m2.Data()) {
		return false
	}
	return isEqualBSON(m1.Data(), m2.Data())
}

func isEqualBSON(m1 map[string]interface{}, m2 map[string]interface{}) bool {
	for k, v := range m1 {
		m2Val := m2[k]
		if reflect.TypeOf(v) != reflect.TypeOf(m2Val) {
			return false
		}
		switch v.(type) {
		case map[string]interface{}, bson.M, data.Data:
			eq := isEqualBSON(v.(bson.M), m2Val.(bson.M))
			if !eq {
				return false
			}
		default:
			eq := reflect.DeepEqual(v, m2Val)
			if !eq {
				return false
			}
		}
	}
	return true
}

func BenchmarkTransformOne(b *testing.B) {
	g := &Goja{Filename: "testdata/transformer.js"}
	msg := message.From(ops.Insert, "collection", map[string]interface{}{"id": bson.NewObjectId(), "name": "nick"})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		g.Apply(msg)
	}
}
