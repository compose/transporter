package state

import (
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/message"
)

func TestFilestore(t *testing.T) {
	fs := NewFilestore("somelongkey", "/tmp/transporter.db", 10000*time.Millisecond)

	data := []struct {
		path string
		in   map[string]interface{}
	}{
		{
			"somepath",
			map[string]interface{}{"id": "nick1", "field1": 1},
		},
		{
			"somepath/morepath",
			map[string]interface{}{"id": "nick1", "field1": 1},
		},
	}

	for _, d := range data {
		err := fs.Save(d.path, message.NewMsg(OpTypeFromString("insert"), d.in)
		if err != nil {
			t.Errorf("got error: %s\n", err)
			t.FailNow()
		}
	}

	for _, d := range data {
		id, ts, err := fs.Get(d.path)
		if err != nil {
			t.Errorf("got error: %s\n", err)
			t.FailNow()
		}
		if !reflect.DeepEqual(id, d.id) {
			t.Errorf("wanted: %s, got: %s", d.id, id)
		}
		if !reflect.DeepEqual(ts, d.ts) {
			t.Errorf("wanted: %s, got: %s", d.ts, ts)
		}
	}

}

func TestFilestoreUpdates(t *testing.T) {
	fs := NewFilestore("somelongkey", "/tmp/transporter.db", 10000*time.Millisecond)

	data := []struct {
		path string
		in   map[string]interface{}
	}{
		{
			"somepath",
			map[string]interface{}{"id": "nick1", "field1": 1},
		},
		{
			"somepath",
			map[string]interface{}{"id": "nick1", "field1": 2},
		},
	}

	for _, d := range data {
		err := fs.Save(d.path, message.NewMsg(OpTypeFromString("insert"), d.in)
		if err != nil {
			t.Errorf("got error: %s\n", err)
			t.FailNow()
		}
	}

	d := data[len(data)-1]
	id, ts, err := fs.Get(d.path)
	if err != nil {
		t.Errorf("got error: %s\n", err)
		t.FailNow()
	}
	if !reflect.DeepEqual(id, d.id) {
		t.Errorf("wanted: %s, got: %s", d.id, id)
	}
	if !reflect.DeepEqual(ts, d.ts) {
		t.Errorf("wanted: %s, got: %s", d.ts, ts)
	}

}
