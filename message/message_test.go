package message

import (
	"reflect"
	"testing"
	"time"

	_ "github.com/compose/transporter/log"
	"github.com/compose/transporter/message/ops"

	"github.com/globalsign/mgo/bson"
)

func TestBase(t *testing.T) {
	testMap := map[string]interface{}{"hello": "world"}
	b := From(ops.Insert, "test", testMap)
	b = WithConfirms(make(chan struct{}), b)
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
	if b.Confirms() == nil {
		t.Errorf("nil confirms channel found")
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

func TestUpdateNamespace(t *testing.T) {
	orig := From(ops.Insert, "foo", nil)
	orig.UpdateNamespace("bar")
	if orig.Namespace() != "bar" {
		t.Errorf("UpdateNamespace failed, expected %s, got %s", "bar", orig.Namespace())
	}
}
