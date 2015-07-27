package adaptor

import (
	"reflect"
	"testing"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
	"gopkg.in/mgo.v2/bson"
)

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
		fn  string
		in  *message.Msg
		out *message.Msg
		err bool
	}{
		{
			// just pass through
			"module.exports=function(doc) { return doc }",
			message.NewMsg(message.Insert, map[string]interface{}{"id": "id1", "name": "nick"}, "database.collection"),
			message.NewMsg(message.Insert, map[string]interface{}{"id": "id1", "name": "nick"}, "database.collection"),
			false,
		},
		{
			// delete the 'name' property
			"module.exports=function(doc) { doc['data'] = _.omit(doc['data'], ['name']); return doc }",
			message.NewMsg(message.Insert, map[string]interface{}{"id": "id2", "name": "nick"}, "database.collection"),
			message.NewMsg(message.Insert, map[string]interface{}{"id": "id2"}, "database.collection"),
			false,
		},
		{
			// delete's should be processed the same
			"module.exports=function(doc) { doc['data'] =  _.omit(doc['data'], ['name']); return doc }",
			message.NewMsg(message.Delete, map[string]interface{}{"id": "id2", "name": "nick"}, "database.collection"),
			message.NewMsg(message.Delete, map[string]interface{}{"id": "id2"}, "database.collection"),
			false,
		},
		{
			// delete's and commands should pass through, and the transformer fn shouldn't run
			"module.exports=function(doc) { return _.omit(doc['data'], ['name']) }",
			message.NewMsg(message.Command, map[string]interface{}{"id": "id2", "name": "nick"}, "database.collection"),
			message.NewMsg(message.Command, map[string]interface{}{"id": "id2", "name": "nick"}, "database.collection"),
			false,
		},
		{
			// bson should marshal and unmarshal properly
			"module.exports=function(doc) { return doc }",
			message.NewMsg(message.Insert, map[string]interface{}{"id": bsonID1, "name": "nick"}, "database.collection"),
			message.NewMsg(message.Insert, map[string]interface{}{"id": bsonID1, "name": "nick"}, "database.collection"),
			false,
		},
		{
			// we should be able to change the bson
			"module.exports=function(doc) { doc['data']['id']['$oid'] = '54a4420502a14b9641000001'; return doc }",
			message.NewMsg(message.Insert, map[string]interface{}{"id": bsonID1, "name": "nick"}, "database.collection"),
			message.NewMsg(message.Insert, map[string]interface{}{"id": bsonID2, "name": "nick"}, "database.collection"),
			false,
		}, {
			// we should be able to skip a nil message
			"module.exports=function(doc) { return false }",
			message.NewMsg(message.Insert, map[string]interface{}{"id": bsonID1, "name": "nick"}, "database.collection"),
			message.NewMsg(message.Noop, map[string]interface{}{"id": bsonID1, "name": "nick"}, "database.collection"),
			false,
		},
		{
			// this throws an error
			"module.exports=function(doc) { return doc['data']['name'] }",
			message.NewMsg(message.Insert, map[string]interface{}{"id": bsonID1, "name": "nick"}, "database.collection"),
			message.NewMsg(message.Insert, "nick", "database.collection"),
			true,
		},
		{
			// we should be able to change the namespace
			"module.exports=function(doc) { doc['ns'] = 'database.table'; return doc }",
			message.NewMsg(message.Insert, map[string]interface{}{"id": bsonID1, "name": "nick"}, "database.collection"),
			message.NewMsg(message.Insert, map[string]interface{}{"id": bsonID1, "name": "nick"}, "database.table"),
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
			t.Errorf("error expected %t but actually got %v", v.err, err)
			continue
		}
		if (!reflect.DeepEqual(msg, v.out) || err != nil) && !v.err {
			t.Errorf("expected:\n(%T) %+v\ngot:\n(%T) %+v with error (%v)\n", v.out, v.out, msg, msg, err)
		}
	}
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

	msg := message.NewMsg(message.Insert, map[string]interface{}{"id": bson.NewObjectId(), "name": "nick"}, "database.collection")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		transformer.transformOne(msg)
	}
}
