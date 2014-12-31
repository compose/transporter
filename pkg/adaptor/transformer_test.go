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
			message.NewMsg(message.Insert, map[string]interface{}{"id": "id1", "name": "nick"}),
			message.NewMsg(message.Insert, map[string]interface{}{"id": "id1", "name": "nick"}),
			false,
		},
		{
			// delete the 'name' property
			"module.exports=function(doc) { return _.omit(doc, ['name']) }",
			message.NewMsg(message.Insert, map[string]interface{}{"id": "id2", "name": "nick"}),
			message.NewMsg(message.Insert, map[string]interface{}{"id": "id2"}),
			false,
		},
		{
			// delete's and commands should pass through, and the transformer fn shouldn't run
			"module.exports=function(doc) { return _.omit(doc, ['name']) }",
			message.NewMsg(message.Delete, map[string]interface{}{"id": "id2", "name": "nick"}),
			message.NewMsg(message.Delete, map[string]interface{}{"id": "id2", "name": "nick"}),
			false,
		},
		{
			// delete's and commands should pass through, and the transformer fn shouldn't run
			"module.exports=function(doc) { return _.omit(doc, ['name']) }",
			message.NewMsg(message.Command, map[string]interface{}{"id": "id2", "name": "nick"}),
			message.NewMsg(message.Command, map[string]interface{}{"id": "id2", "name": "nick"}),
			false,
		},
		{
			// bson should marshal and unmarshal properly
			"module.exports=function(doc) { return doc }",
			message.NewMsg(message.Insert, map[string]interface{}{"id": bsonID1, "name": "nick"}),
			message.NewMsg(message.Insert, map[string]interface{}{"id": bsonID1, "name": "nick"}),
			false,
		},
		{
			// we should be able to change the bson
			"module.exports=function(doc) { doc['id']['$oid'] = '54a4420502a14b9641000001'; return doc }",
			message.NewMsg(message.Insert, map[string]interface{}{"id": bsonID1, "name": "nick"}),
			message.NewMsg(message.Insert, map[string]interface{}{"id": bsonID2, "name": "nick"}),
			false,
		},
		{
			// we should be able to change the bson
			"module.exports=function(doc) { return doc['name'] }",
			message.NewMsg(message.Insert, map[string]interface{}{"id": bsonID1, "name": "nick"}),
			message.NewMsg(message.Insert, "nick"),
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
		if !reflect.DeepEqual(msg.Data, v.out.Data) || err != nil {
			t.Errorf("expected:\n(%T) %+v\ngot:\n(%T) %+v with error (%v)\n", v.out.Data, v.out.Data, msg.Data, msg.Data, err)
		}
	}
}
