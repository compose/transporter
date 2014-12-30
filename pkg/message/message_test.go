package message

import (
	"testing"

	"gopkg.in/mgo.v2/bson"
)

type FakeMessage struct {
	Op  OpType
	Doc bson.M
}

func TestIdString(t *testing.T) {
	data := []struct {
		in   map[string]interface{}
		key  string
		want string
	}{
		{
			nil,
			"id",
			"",
		},

		{
			map[string]interface{}{"field0": 1},
			"id",
			"",
		},
		{
			map[string]interface{}{"id": "nick1", "field1": 1},
			"id",
			"nick1",
		},
		{
			map[string]interface{}{"_id": "nick2", "field2": 1},
			"_id",
			"nick2",
		},
	}

	for _, v := range data {
		msg := NewMsg(OpTypeFromString("insert"), v.in)
		if msg.IDString(v.key) != v.want {
			t.Errorf("IdString failed.  expected %+v, got %+v", v.want, msg.IDString(v.key))
		}
	}
}
