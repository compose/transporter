package message

import (
	"github.com/compose/transporter/pkg/message/ops"

	"gopkg.in/mgo.v2/bson"
)

type FakeMessage struct {
	Op  ops.Op
	Doc bson.M
}

// func TestIdString(t *testing.T) {
// 	data := []struct {
// 		in   map[string]interface{}
// 		key  string
// 		want string
// 		err  bool
// 	}{
// 		{
// 			nil,
// 			"id",
// 			"",
// 			true,
// 		},
//
// 		{
// 			map[string]interface{}{"field0": 1},
// 			"id",
// 			"",
// 			true,
// 		},
// 		{
// 			map[string]interface{}{"id": "nick1", "field1": 1},
// 			"id",
// 			"nick1",
// 			false,
// 		},
// 		{
// 			map[string]interface{}{"_id": "nick2", "field2": 1},
// 			"_id",
// 			"nick2",
// 			false,
// 		},
// 	}
//
// 	for _, v := range data {
// 		msg := NewMsg(OpTypeFromString("insert"), v.in, "database.collection")
// 		id, err := msg.IDString(v.key)
// 		if (err != nil) != v.err {
// 			t.Errorf("expected error: %t, but got error: %v", v.err, err)
// 		}
// 		if id != v.want {
// 			t.Errorf("IdString failed.  expected %+v, got %+v", v.want, id)
// 		}
// 	}
// }
