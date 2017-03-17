package pretty

import (
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/function"
	_ "github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"

	bson "gopkg.in/mgo.v2/bson"
)

var initTests = []struct {
	in     map[string]interface{}
	expect *Prettify
}{
	{map[string]interface{}{}, DefaultPrettifier},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := function.GetFunction("pretty", it.in)
		if err != nil {
			t.Fatalf("unexpected GetFunction() error, %s", err)
		}
		if !reflect.DeepEqual(a, it.expect) {
			t.Errorf("misconfigured Function, expected %+v, got %+v", it.expect, a)
		}
	}
}

var prettyTests = []struct {
	p    *Prettify
	data map[string]interface{}
}{
	{
		DefaultPrettifier,
		map[string]interface{}{"_id": "blah", "type": "good"},
	},
	{
		DefaultPrettifier,
		map[string]interface{}{"_id": "blah", "type": "good", "name": "hello"},
	},
	{
		DefaultPrettifier,
		map[string]interface{}{"_id": bson.NewObjectId(), "hello": "world", "ts": bson.MongoTimestamp(time.Now().Unix() << 32)},
	},
	{
		&Prettify{Spaces: 0},
		map[string]interface{}{"_id": bson.NewObjectId(), "hello": "world", "ts": bson.MongoTimestamp(time.Now().Unix() << 32)},
	},
}

func TestApply(t *testing.T) {
	for _, pt := range prettyTests {
		msg, err := pt.p.Apply(message.From(ops.Insert, "test", pt.data))
		if err != nil {
			t.Errorf("unexpected error, got %s", err)
		}
		if !reflect.DeepEqual(msg.Data().AsMap(), pt.data) {
			t.Errorf("wrong message, expected %+v, got %+v", pt.data, msg.Data().AsMap())
		}
	}
}
