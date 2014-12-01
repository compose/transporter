package message

import (
	"reflect"
	"testing"

	"gopkg.in/mgo.v2/bson"
)

type FakeMessage struct {
	Op  OpType
	Doc bson.M
}

func TestNewMsg(t *testing.T) {
	data := []struct {
		in  FakeMessage
		out *Msg
	}{
		{
			FakeMessage{Op: Insert, Doc: nil},
			&Msg{Op: Insert, Id: nil, document: nil},
		},
		{
			FakeMessage{Op: Command, Doc: bson.M{"field1": 1}},
			&Msg{Op: Command, Id: nil, document: bson.M{"field1": 1}},
		},
		{
			FakeMessage{Op: Insert, Doc: bson.M{"id": "nick", "field2": 1}},
			&Msg{Op: Insert, Id: "nick", document: bson.M{"field2": 1}, idKey: "id"},
		},
		{
			FakeMessage{Op: Insert, Doc: bson.M{"_id": "nick", "field2": 1}},
			&Msg{Op: Insert, Id: "nick", document: bson.M{"field2": 1}, idKey: "_id"},
		},
	}

	for _, v := range data {
		m := NewMsg(v.in.Op, v.in.Doc)

		if !reflect.DeepEqual(m.Document(), v.out.Document()) {
			t.Errorf("Bad doc.  expected %v, got %v", v.out.Document(), m.Document())
		}

		if !reflect.DeepEqual(m.Id, v.out.Id) {
			t.Errorf("Bad Id.  expected %v, got %v", v.out.Id, m.Id)
		}
	}
}

func TestDocument(t *testing.T) {
	data := []struct {
		in  *Msg
		out bson.M
	}{
		{
			NewMsg(Insert, nil),
			nil,
		},
		{
			NewMsg(Insert, bson.M{"field": 1}),
			bson.M{"field": 1},
		},
		{
			NewMsg(Insert, bson.M{"id": "nick", "field": 1}),
			bson.M{"id": "nick", "field": 1},
		},
		{
			NewMsg(Insert, bson.M{"_id": "nick", "field": 1}),
			bson.M{"_id": "nick", "field": 1},
		},
	}

	for _, v := range data {
		if !reflect.DeepEqual(v.in.Document(), v.out) {
			t.Errorf("Bad doc.  expected %+v, got %+v", v.out, v.in.Document())
		}
	}
}

func TestDocumentWithId(t *testing.T) {
	data := []struct {
		in    *Msg
		idkey string
		out   bson.M
	}{
		{
			NewMsg(Insert, nil),
			"_id",
			nil,
		},

		{
			NewMsg(Insert, bson.M{"field": 1}),
			"_id",
			bson.M{"field": 1},
		},
		{
			NewMsg(Insert, bson.M{"id": "nick", "field": 1}),
			"id",
			bson.M{"id": "nick", "field": 1},
		},
		{
			NewMsg(Insert, bson.M{"id": "nick", "field": 1}),
			"_id",
			bson.M{"_id": "nick", "field": 1},
		},
		{
			NewMsg(Insert, bson.M{"_id": "nick", "field": 1}),
			"id",
			bson.M{"id": "nick", "field": 1},
		},
		{
			NewMsg(Insert, bson.M{"id": "nick", "field": 1}),
			"_id",
			bson.M{"_id": "nick", "field": 1},
		},
	}

	for _, v := range data {
		if !reflect.DeepEqual(v.in.DocumentWithId(v.idkey), v.out) {
			t.Errorf("Bad doc.  expected %+v, got %+v", v.out, v.in.DocumentWithId(v.idkey))
		}
	}
}

func TestOriginalIdOnNew(t *testing.T) {
	data := []struct {
		in         bson.M
		originalId interface{}
	}{
		{
			nil,
			nil,
		},

		{
			bson.M{"field0": 1},
			nil,
		},
		{
			bson.M{"id": "nick1", "field1": 1},
			"nick1",
		},
		{
			bson.M{"_id": "nick2", "field2": 1},
			"nick2",
		},
	}

	for _, v := range data {
		msg := NewMsg(OpTypeFromString("insertable"), v.in)
		if msg.OriginalId != v.originalId {
			t.Errorf("NewMsg failed.  expected %+v, got %+v", v.originalId, msg.OriginalId)
		}
	}
}

func TestOriginalIdOnSet(t *testing.T) {
	data := []struct {
		in         bson.M
		originalId interface{}
	}{
		{
			nil,
			nil,
		},

		{
			bson.M{"field0": 1},
			nil,
		},
		{
			bson.M{"id": "nick1", "field1": 1},
			"nick1",
		},
		{
			bson.M{"_id": "nick2", "field2": 1},
			"nick2",
		},
	}

	for _, v := range data {
		msg := NewMsg(OpTypeFromString("inserty"), nil)
		msg.SetDocument(v.in)
		if msg.OriginalId != v.originalId {
			t.Errorf("SetDocument failed.  expected %+v, got %+v", v.originalId, msg.OriginalId)
		}
	}
}
