package state

import (
	"encoding/gob"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/message"
)

type testMsg struct {
	namespace string
	data      map[string]interface{}
	ts        int64
}

func (t testMsg) Namespace() string {
	return t.namespace
}

func (t testMsg) Data() interface{} {
	return t.data
}

func (t testMsg) Timestamp() int64 {
	return t.ts
}

func TestFilestore(t *testing.T) {
	fs := NewFilestore("somelongkey", "/tmp/transporter.state")

	data := []struct {
		path string
		in   message.Msg
		out  message.Msg
	}{
		{
			"somepath",
			testMsg{
				ts:        time.Now().Unix(),
				data:      map[string]interface{}{"id": "nick1", "field1": 1},
				namespace: "db.coll",
			},
			testMsg{
				ts:        time.Now().Unix(),
				data:      map[string]interface{}{"id": "nick1", "field1": 1},
				namespace: "db.coll",
			},
		},
		{
			"somepath/morepath",
			testMsg{
				ts:        time.Now().Unix(),
				data:      map[string]interface{}{"id": "nick1", "field1": 1},
				namespace: "db.coll",
			},
			testMsg{
				ts:        time.Now().Unix(),
				data:      map[string]interface{}{"id": "nick1", "field1": 1},
				namespace: "db.coll",
			},
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
			t.Errorf("wanted: %#v, got: %#v", d.out, out.Msg)
		}
	}

}

func TestFilestoreUpdates(t *testing.T) {
	fs := NewFilestore("somelongkey", "/tmp/transporter.state")

	data := []struct {
		path string
		in   message.Msg
		out  message.Msg
	}{
		{
			"somepath",
			testMsg{
				ts:        time.Now().Unix(),
				data:      map[string]interface{}{"id": "nick1", "field1": 1},
				namespace: "db.coll",
			},
			testMsg{
				ts:        time.Now().Unix(),
				data:      map[string]interface{}{"id": "nick1", "field1": 1},
				namespace: "db.coll",
			},
		},
		{
			"somepath",
			testMsg{
				ts:        time.Now().Unix(),
				data:      map[string]interface{}{"id": "nick1", "field1": 2},
				namespace: "db.coll",
			},
			testMsg{
				ts:        time.Now().Unix(),
				data:      map[string]interface{}{"id": "nick1", "field1": 2},
				namespace: "db.coll",
			},
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
		t.Errorf("wanted: %#v, got: %#v", d.out, out.Msg)
	}

}
