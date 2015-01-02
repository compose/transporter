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
			message.NewMsg(message.Insert, map[string]interface{}{"id": "nick1", "field1": 1}),
			message.NewMsg(message.Insert, map[string]interface{}{"id": "nick1", "field1": 1}),
		},
		{
			"somepath/morepath",
			message.NewMsg(message.Insert, map[string]interface{}{"id": "nick1", "field1": 1}),
			message.NewMsg(message.Insert, map[string]interface{}{"id": "nick1", "field1": 1}),
		},
	}

	for _, d := range data {
		err := fs.Set(d.path, d.in)
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
		if !reflect.DeepEqual(out, d.out) {
			t.Errorf("wanted: %s, got: %s", d.out, out)
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
			message.NewMsg(message.Insert, map[string]interface{}{"id": "nick1", "field1": 1}),
			message.NewMsg(message.Insert, map[string]interface{}{"id": "nick1", "field1": 1}),
		},
		{
			"somepath",
			message.NewMsg(message.Insert, map[string]interface{}{"id": "nick1", "field1": 2}),
			message.NewMsg(message.Insert, map[string]interface{}{"id": "nick1", "field1": 2}),
		},
	}

	for _, d := range data {
		err := fs.Set(d.path, d.in)
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
	states := make(map[string]*message.Msg)
	dec := gob.NewDecoder(fh)
	err = dec.Decode(&states)
	if err != nil {
		t.Errorf("got error: %s\n", err)
		t.FailNow()
	}
	out := states["somelongkey-somepath"]
	if !reflect.DeepEqual(out, d.out) {
		t.Errorf("wanted: %s, got: %s", d.out, out)
	}

}
