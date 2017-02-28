package state

import (
	"encoding/gob"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
)

type testMsg struct {
	UniqueID  string
	Operation ops.Op
	NS        string
	D         data.Data
	TS        int64
}

func (t testMsg) ID() string {
	return t.UniqueID
}

func (t testMsg) OP() ops.Op {
	return t.Operation
}

func (t testMsg) Namespace() string {
	return t.NS
}

func (t testMsg) Data() data.Data {
	return t.D
}

func (t testMsg) Timestamp() int64 {
	return t.TS
}

func TestFilestore(t *testing.T) {
	gob.Register(testMsg{})
	fs := NewFilestore("somelongkey", "/tmp/transporter.state")

	data := []struct {
		path string
		in   message.Msg
		out  message.Msg
	}{
		{
			"somepath",
			testMsg{
				TS: time.Now().Unix(),
				D:  map[string]interface{}{"id": "nick1", "field1": 1},
				NS: "db.coll",
			},
			testMsg{
				TS: time.Now().Unix(),
				D:  map[string]interface{}{"id": "nick1", "field1": 1},
				NS: "db.coll",
			},
		},
		{
			"somepath/morepath",
			testMsg{
				TS: time.Now().Unix(),
				D:  map[string]interface{}{"id": "nick1", "field1": 1},
				NS: "db.coll",
			},
			testMsg{
				TS: time.Now().Unix(),
				D:  map[string]interface{}{"id": "nick1", "field1": 1},
				NS: "db.coll",
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
				TS: time.Now().Unix(),
				D:  map[string]interface{}{"id": "nick1", "field1": 1},
				NS: "db.coll",
			},
			testMsg{
				TS: time.Now().Unix(),
				D:  map[string]interface{}{"id": "nick1", "field1": 1},
				NS: "db.coll",
			},
		},
		{
			"somepath",
			testMsg{
				TS: time.Now().Unix(),
				D:  map[string]interface{}{"id": "nick1", "field1": 2},
				NS: "db.coll",
			},
			testMsg{
				TS: time.Now().Unix(),
				D:  map[string]interface{}{"id": "nick1", "field1": 2},
				NS: "db.coll",
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
