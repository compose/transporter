package message

import (
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/message/ops"

	"gopkg.in/mgo.v2/bson"
)

func TestBase(t *testing.T) {
	testMap := map[string]interface{}{"hello": "world"}
	b := From(ops.Insert, "test", testMap)
	if b.OP() != ops.Insert {
		t.Errorf("wrong Op, expected %+v, got %+v", ops.Insert, b.OP())
	}
	if b.Namespace() != "test" {
		t.Errorf("wrong Namespace, expected test, got %+v", b.Namespace())
	}
	now := time.Now().Unix()
	if b.Timestamp() > now {
		t.Errorf("bad Timestamp, should be after %d but was %d", now, b.Timestamp())
	}
	if !reflect.DeepEqual(b.Data().AsMap(), testMap) {
		t.Errorf("bad data, expected %+v, got %+v", testMap, b.Data())
	}
}

func TestID(t *testing.T) {
	data := []struct {
		in   map[string]interface{}
		key  string
		want string
	}{
		{
			map[string]interface{}{"id": "nick1", "field1": 1},
			"id",
			"",
		},
		{
			map[string]interface{}{"_id": "nick2", "field2": 1},
			"_id",
			"nick2",
		},
		{
			map[string]interface{}{"_id": bson.ObjectIdHex("58b4e84646a2d647a4780812"), "field2": 1},
			"_id",
			"58b4e84646a2d647a4780812",
		},
		{
			map[string]interface{}{"_id": 1, "field2": 1},
			"_id",
			"1",
		},
	}

	for _, v := range data {
		msg := From(ops.Insert, "collection", v.in)
		if msg.ID() != v.want {
			t.Errorf("ID() failed, expected %+v, got %+v", v.want, msg.ID())
		}
	}
}
