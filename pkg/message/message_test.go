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
		in   bson.M
		key  string
		want string
	}{
		{
			nil,
			"id",
			"",
		},

		{
			bson.M{"field0": 1},
			"id",
			"",
		},
		{
			bson.M{"id": "nick1", "field1": 1},
			"id",
			"nick1",
		},
		{
			bson.M{"_id": "nick2", "field2": 1},
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
