package state

import (
	"encoding/gob"
	"os"
	"reflect"
	"testing"

	"github.com/compose/transporter/pkg/message"
)

func TestFilestore(t *testing.T) {
	fs := NewFilestore("somelongkey", "/tmp/transporter.state")

	data := []struct {
		path string
		in   *message.Msg
		out  *message.Msg
	}{
		{
			"somepath",
			message.NewMsg(message.Insert, map[string]interface{}{"id": "nick1", "field1": 1}, "db.coll"),
			message.NewMsg(message.Insert, map[string]interface{}{"id": "nick1", "field1": 1}, "db.coll"),
		},
		{
			"somepath/morepath",
			message.NewMsg(message.Insert, map[string]interface{}{"id": "nick1", "field1": 1}, "db.coll"),
			message.NewMsg(message.Insert, map[string]interface{}{"id": "nick1", "field1": 1}, "db.coll"),
		},
	}

	for _, d := range data {
		err := fs.Set(d.path, &MsgState{Msg: d.in, Extra: make(map[string]interface{})})
		if err != nil {
			t.Errorf("got error: %s\n", err)
			t.FailNow()
		}
	}

	for _, d := range data {
		out, err := fs.Get(d.path)
		if err != nil {
			t.Errorf("got error: %s\n", err)
			t.FailNow()
		}
		if !reflect.DeepEqual(out.Msg, d.out) {
			t.Errorf("wanted: %s, got: %s", d.out, out.Msg)
		}
	}

}

func TestFilestoreUpdates(t *testing.T) {
	fs := NewFilestore("somelongkey", "/tmp/transporter.state")

	data := []struct {
		path string
		in   *message.Msg
		out  *message.Msg
	}{
		{
			"somepath",
			message.NewMsg(message.Insert, map[string]interface{}{"id": "nick1", "field1": 1}, "db.coll"),
			message.NewMsg(message.Insert, map[string]interface{}{"id": "nick1", "field1": 1}, "db.coll"),
		},
		{
			"somepath",
			message.NewMsg(message.Insert, map[string]interface{}{"id": "nick1", "field1": 2}, "db.coll"),
			message.NewMsg(message.Insert, map[string]interface{}{"id": "nick1", "field1": 2}, "db.coll"),
		},
	}

	for _, d := range data {
		err := fs.Set(d.path, &MsgState{Msg: d.in, Extra: make(map[string]interface{})})
		if err != nil {
			t.Errorf("got error: %s\n", err)
			t.FailNow()
		}
	}

	d := data[len(data)-1]
	fh, err := os.Open("/tmp/transporter.state")
	if err != nil {
		t.Errorf("got error: %s\n", err)
		t.FailNow()
	}
	states := make(map[string]*MsgState)
	dec := gob.NewDecoder(fh)
	err = dec.Decode(&states)
	if err != nil {
		t.Errorf("got error: %s\n", err)
		t.FailNow()
	}
	out := states["somelongkey-somepath"]
	if !reflect.DeepEqual(out.Msg, d.out) {
		t.Errorf("wanted: %s, got: %s", d.out, out.Msg)
	}

}
