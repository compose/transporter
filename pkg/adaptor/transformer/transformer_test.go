package transformer

import (
	"reflect"
	"testing"

	"github.com/compose/transporter/pkg/message"
	_ "github.com/compose/transporter/pkg/message/adaptor/transformer"
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
	"github.com/compose/transporter/pkg/pipe"
	"gopkg.in/mgo.v2/bson"
)

type testMessage struct {
	id string
	op ops.Op
	ts int64
	d  data.Data
	ns string
}

func (t testMessage) ID() string {
	return t.id
}

func (t testMessage) OP() ops.Op {
	return t.op
}

func (t testMessage) Timestamp() int64 {
	return t.ts
}

func (t testMessage) Data() data.Data {
	return t.d
}

func (t testMessage) Namespace() string {
	return t.ns
}

func TestTransformOne(t *testing.T) {
	bsonID1 := bson.NewObjectId()
	bsonID2 := bson.ObjectIdHex("54a4420502a14b9641000001")
	tpipe := pipe.NewPipe(nil, "path")
	go func(p *pipe.Pipe) {
		for range p.Err {
			// noop
		}
	}(tpipe)

	data := []struct {
		name string
		fn   string
		in   message.Msg
		out  message.Msg
		err  bool
	}{
		{
			"just pass through",
			"module.exports=function(doc) { return doc }",
			message.MustUseAdaptor("transformer").From(ops.Insert, "database.collection", data.Data{"id": "id1", "name": "nick"}),
			message.MustUseAdaptor("transformer").From(ops.Insert, "database.collection", data.Data{"id": "id1", "name": "nick"}),
			false,
		},
		{
			"delete the 'name' property",
			"module.exports=function(doc) { doc['data'] = _.omit(doc['data'], ['name']); return doc }",
			message.MustUseAdaptor("transformer").From(ops.Insert, "database.collection", data.Data{"id": "id2", "name": "nick"}),
			message.MustUseAdaptor("transformer").From(ops.Insert, "database.collection", data.Data{"id": "id2"}),
			false,
		},
		{
			"delete's should be processed the same",
			"module.exports=function(doc) { doc['data'] =  _.omit(doc['data'], ['name']); return doc }",
			message.MustUseAdaptor("transformer").From(ops.Delete, "database.collection", data.Data{"id": "id2", "name": "nick"}),
			message.MustUseAdaptor("transformer").From(ops.Delete, "database.collection", data.Data{"id": "id2"}),
			false,
		},
		{
			"delete's and commands should pass through, and the transformer fn shouldn't run",
			"module.exports=function(doc) { return _.omit(doc['data'], ['name']) }",
			message.MustUseAdaptor("transformer").From(ops.Command, "database.collection", data.Data{"id": "id2", "name": "nick"}),
			message.MustUseAdaptor("transformer").From(ops.Command, "database.collection", data.Data{"id": "id2", "name": "nick"}),
			false,
		},
		{
			"bson should marshal and unmarshal properly",
			"module.exports=function(doc) { return doc }",
			message.MustUseAdaptor("transformer").From(ops.Insert, "database.collection", data.Data{"id": bsonID1, "name": "nick"}),
			message.MustUseAdaptor("transformer").From(ops.Insert, "database.collection", data.Data{"id": bsonID1, "name": "nick"}),
			false,
		},
		{
			"we should be able to change the bson",
			"module.exports=function(doc) { doc['data']['id']['$oid'] = '54a4420502a14b9641000001'; return doc }",
			message.MustUseAdaptor("transformer").From(ops.Insert, "database.collection", data.Data{"id": bsonID1, "name": "nick"}),
			message.MustUseAdaptor("transformer").From(ops.Insert, "database.collection", data.Data{"id": bsonID2, "name": "nick"}),
			false,
		}, {
			"we should be able to skip a nil message",
			"module.exports=function(doc) { return false }",
			message.MustUseAdaptor("transformer").From(ops.Insert, "database.collection", data.Data{"id": bsonID1, "name": "nick"}),
			nil,
			false,
		},
		{
			"we should be able to change the namespace",
			"module.exports=function(doc) { doc['ns'] = 'database.table'; return doc }",
			message.MustUseAdaptor("transformer").From(ops.Insert, "database.collection", data.Data{"id": bsonID1, "name": "nick"}),
			message.MustUseAdaptor("transformer").From(ops.Insert, "database.table", data.Data{"id": bsonID1, "name": "nick"}),
			false,
		}, {
			"we should be able to add an object to the bson",
			`module.exports=function(doc) { doc['data']['added'] = {"name":"batman","villain":"joker"}; return doc }`,
			message.MustUseAdaptor("transformer").From(ops.Insert, "database.collection", data.Data{"name": "nick"}),
			message.MustUseAdaptor("transformer").From(ops.Insert, "database.collection", data.Data{"name": "nick", "added": bson.M{"name": "batman", "villain": "joker"}}),
			false,
		},
	}
	for _, v := range data {
		transformer := &Transformer{pipe: tpipe, path: "path", fn: v.fn}
		err := transformer.initEnvironment()
		if err != nil {
			panic(err)
		}
		msg, err := transformer.transformOne(v.in)

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
	tpipe := pipe.NewPipe(nil, "path")
	transformer := &Transformer{
		pipe: tpipe,
		path: "path",
		fn:   "module.exports=function(doc) { return doc }",
	}
	err := transformer.initEnvironment()
	if err != nil {
		panic(err)
	}

	msg := testMessage{
		op: ops.Insert,
		d:  map[string]interface{}{"id": bson.NewObjectId(), "name": "nick"},
		ns: "database.collection",
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		transformer.transformOne(msg)
	}
}
