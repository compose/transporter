package ottojs

import (
	"reflect"
	"testing"

	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
	"github.com/globalsign/mgo/bson"
)

var (
	bsonID1    = bson.NewObjectId()
	bsonID2    = bson.ObjectIdHex("54a4420502a14b9641000001")
	writeTests = []struct {
		name string
		fn   string
		in   message.Msg
		out  message.Msg
		err  bool
	}{
		{
			"just pass through",
			"testdata/transformer.js",
			message.From(ops.Insert, "collection", data.Data{"id": "id1", "name": "nick"}),
			message.From(ops.Insert, "collection", data.Data{"id": "id1", "name": "nick"}),
			false,
		},
		{
			"delete the 'name' property",
			"testdata/delete_name.js",
			message.From(ops.Insert, "collection", data.Data{"id": "id2", "name": "nick"}),
			message.From(ops.Insert, "collection", data.Data{"id": "id2"}),
			false,
		},
		{
			"delete's should be processed the same",
			"testdata/delete_name.js",
			message.From(ops.Delete, "collection", data.Data{"id": "id2", "name": "nick"}),
			message.From(ops.Delete, "collection", data.Data{"id": "id2"}),
			false,
		},
		{
			"bson should marshal and unmarshal properly",
			"testdata/transformer.js",
			message.From(ops.Insert, "collection", data.Data{"id": bsonID1, "name": "nick"}),
			message.From(ops.Insert, "collection", data.Data{"id": bsonID1, "name": "nick"}),
			false,
		},
		{
			"we should be able to change the bson",
			"testdata/change_bson.js",
			message.From(ops.Insert, "collection", data.Data{"id": bsonID1, "name": "nick"}),
			message.From(ops.Insert, "collection", data.Data{"id": bsonID2, "name": "nick"}),
			false,
		}, {
			"we should be able to skip a nil message",
			"testdata/skip.js",
			message.From(ops.Insert, "collection", data.Data{"id": bsonID1, "name": "nick"}),
			nil,
			false,
		},
		{
			"we should be able to change the namespace",
			"testdata/change_ns.js",
			message.From(ops.Insert, "collection", data.Data{"id": bsonID1, "name": "nick"}),
			message.From(ops.Insert, "table", data.Data{"id": bsonID1, "name": "nick"}),
			false,
		}, {
			"we should be able to add an object to the bson",
			"testdata/add_data.js",
			message.From(ops.Insert, "collection", data.Data{"name": "nick"}),
			message.From(ops.Insert, "collection", data.Data{"name": "nick", "added": bson.M{"name": "batman", "villain": "joker"}}),
			false,
		},
	}
)

func TestApply(t *testing.T) {
	for _, v := range writeTests {
		o := otto{Filename: v.fn}
		msg, err := o.Apply(v.in)
		if (err != nil) != v.err {
			t.Errorf("[%s] error expected %t but actually got %v", v.name, v.err, err)
			continue
		}
		if (!isEqual(msg, v.out) || err != nil) && !v.err {
			t.Errorf("[%s] expected:\n(%T) %+v\ngot:\n(%T) %+v with error (%v)\n", v.name, v.out, v.out, msg, msg, err)
		}
	}
}

func isEqual(m1 message.Msg, m2 message.Msg) bool {
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
	o := otto{Filename: "testdata/transformer.js"}
	msg := message.From(ops.Insert, "collection", map[string]interface{}{"id": bson.NewObjectId(), "name": "nick"})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		o.Apply(msg)
	}
}
