package pretty

import (
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/adaptor"
	_ "github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"

	bson "gopkg.in/mgo.v2/bson"
)

func TestInit(t *testing.T) {
	a, err := adaptor.GetAdaptor("pretty", map[string]interface{}{})
	if err != nil {
		t.Fatalf("unexpected GetAdaptor() error, %s", err)
	}
	if _, err := a.Client(); err != nil {
		t.Errorf("unexpected Client() error, %s", err)
	}
	rerr := adaptor.ErrFuncNotSupported{Name: "transformer", Func: "Reader()"}
	if _, err := a.Reader(); err != rerr {
		t.Errorf("wrong Reader() error, expected %s, got %s", rerr, err)
	}
	if _, err := a.Writer(nil, nil); err != nil {
		t.Errorf("unexpected Writer() error, %s", err)
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

func TestPretty(t *testing.T) {
	for _, pt := range prettyTests {
		msg, err := pt.p.Write(message.From(ops.Insert, "test", pt.data))(nil)
		if err != nil {
			t.Errorf("unexpected error, got %s", err)
		}
		if !reflect.DeepEqual(msg.Data().AsMap(), pt.data) {
			t.Errorf("wrong message, expected %+v, got %+v", pt.data, msg.Data().AsMap())
		}
	}
}
