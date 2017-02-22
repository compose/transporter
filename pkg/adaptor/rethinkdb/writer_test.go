package rethinkdb

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"

	r "gopkg.in/gorethink/gorethink.v3"
)

var optests = []struct {
	op         ops.Op
	registered bool
}{
	{ops.Insert, true},
	{ops.Update, true},
	{ops.Delete, true},
	{ops.Command, false},
	{ops.Noop, false},
}

func TestOpFunc(t *testing.T) {
	w := newWriter("test")
	for _, ot := range optests {
		if _, ok := w.writeMap[ot.op]; ok != ot.registered {
			t.Errorf("op (%s) registration incorrect, expected %+v, got %+v\n", ot.op.String(), ot.registered, ok)
		}
	}
}

var (
	writerTestData = &TestData{"writer_test", "test", 0}

	inserttests = []struct {
		table            string
		data             []data.Data
		docCount         int
		verifyLastInsert bool
	}{
		{
			"simple",
			[]data.Data{map[string]interface{}{"hello": "world"}},
			1,
			false,
		},
		{
			"lotsodata",
			[]data.Data{
				map[string]interface{}{"hello": "world"},
				map[string]interface{}{"hello": "world"},
				map[string]interface{}{"hello": "world"},
				map[string]interface{}{"hello": "world"},
				map[string]interface{}{"hello": "world"},
			},
			5,
			false,
		},
		{
			"withupdate",
			[]data.Data{
				map[string]interface{}{"id": "f21eb576-d3ff-4cd8-a419-2d770d20e800", "hello": "world"},
				map[string]interface{}{"id": "c18adb45-ae9d-4673-b3c4-6a27b045727a", "bonjour": "world"},
				map[string]interface{}{"id": "015d66a2-0b09-483e-b2f5-2973d9e4f0b3", "hola": "world"},
				map[string]interface{}{"id": "8a7b9683-3a86-49c4-a6a3-08b856025244", "guten tag": "world"},
				map[string]interface{}{"id": "f21eb576-d3ff-4cd8-a419-2d770d20e800", "hello": "moar world"},
			},
			4,
			true,
		},
	}
)

func TestInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Insert in short mode")
	}

	w := newWriter(writerTestData.DB)
	for _, it := range inserttests {

		if _, err := r.DB(writerTestData.DB).TableCreate(it.table).RunWrite(defaultSession.session); err != nil {
			log.Errorf("failed to create table (%s) in %s, may affect tests!, %s", it.table, writerTestData.DB, err)
		}

		for _, data := range it.data {
			msg := message.From(ops.Insert, fmt.Sprintf("%s.%s", writerTestData.DB, it.table), data)
			if err := w.Write(msg)(defaultSession); err != nil {
				t.Errorf("unexpected Insert error, %s\n", err)
			}
		}
		countResp, err := r.DB(writerTestData.DB).Table(it.table).Count().Run(defaultSession.session)
		if err != nil {
			t.Errorf("unable to determine table count, %s\n", err)
		}
		var count int
		countResp.One(&count)
		countResp.Close()
		if count != it.docCount {
			t.Errorf("mismatched doc count, expected %d, got %d\n", it.docCount, count)
		}
		if it.verifyLastInsert {
			var result map[string]interface{}
			lastDoc := it.data[len(it.data)-1]
			cursor, err := r.DB(writerTestData.DB).Table(it.table).Get(lastDoc.Get("id")).Run(defaultSession.session)
			if err != nil {
				t.Fatalf("unexpected Get error, %s\n", err)
			}
			cursor.One(&result)
			cursor.Close()
			if !reflect.DeepEqual(lastDoc.AsMap(), result) {
				t.Errorf("mismatched document, expected %+v (%T), got %+v (%T)\n", lastDoc.AsMap(), lastDoc.AsMap(), result, result)
			}
		}
	}
}

var updatetests = []struct {
	table       string
	id          string
	originalDoc data.Data
	updatedDoc  data.Data
}{
	{
		"updatesimple",
		"4e9e5bc2-9b11-4143-9aa1-75c10e7a193a",
		map[string]interface{}{"hello": "world"},
		map[string]interface{}{"hello": "again"},
	},
}

func TestUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Update in short mode")
	}
	w := newWriter(writerTestData.DB)
	for _, ut := range updatetests {
		if _, err := r.DB(writerTestData.DB).TableCreate(ut.table).RunWrite(defaultSession.session); err != nil {
			log.Errorf("failed to create table (%s) in %s, may affect tests!, %s", ut.table, writerTestData.DB, err)
		}

		ns := fmt.Sprintf("%s.%s", writerTestData.DB, ut.table)
		// Insert data
		ut.originalDoc.Set("id", ut.id)
		msg := message.From(ops.Insert, ns, ut.originalDoc)
		if err := w.Write(msg)(defaultSession); err != nil {
			t.Errorf("unexpected Insert error, %s\n", err)
		}
		// Update data
		ut.updatedDoc.Set("id", ut.id)
		msg = message.From(ops.Update, ns, ut.updatedDoc)
		if err := w.Write(msg)(defaultSession); err != nil {
			t.Errorf("unexpected Update error, %s\n", err)
		}
		// Validate update
		expectedDoc := map[string]interface{}{"id": ut.id}
		for k, v := range ut.updatedDoc {
			expectedDoc[k] = v
		}
		var result map[string]interface{}
		cursor, err := r.DB(writerTestData.DB).Table(ut.table).Get(ut.id).Run(defaultSession.session)
		if err != nil {
			t.Fatalf("unexpected Get error, %s\n", err)
		}
		cursor.One(&result)
		cursor.Close()
		if !reflect.DeepEqual(expectedDoc, result) {
			t.Errorf("mismatched document, expected %+v (%T), got %+v (%T)\n", expectedDoc, expectedDoc, result, result)
		}
	}
}

var deletetests = []struct {
	table       string
	id          string
	originalDoc data.Data
}{
	{
		"deletesimple",
		"4e9e5bc2-9b11-4143-9aa1-75c10e7a193a",
		map[string]interface{}{"hello": "world"},
	},
}

func TestDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Delete in short mode")
	}
	w := newWriter(writerTestData.DB)
	for _, dt := range deletetests {
		if _, err := r.DB(writerTestData.DB).TableCreate(dt.table).RunWrite(defaultSession.session); err != nil {
			log.Errorf("failed to create table (%s) in %s, may affect tests!, %s", dt.table, writerTestData.DB, err)
		}

		ns := fmt.Sprintf("%s.%s", writerTestData.DB, dt.table)
		// Insert data
		dt.originalDoc.Set("_id", dt.id)
		msg := message.From(ops.Insert, ns, dt.originalDoc)
		if err := w.Write(msg)(defaultSession); err != nil {
			t.Errorf("unexpected Insert error, %s\n", err)
		}
		// Delete data
		msg = message.From(ops.Delete, ns, dt.originalDoc)
		if err := w.Write(msg)(defaultSession); err != nil {
			t.Errorf("unexpected Delete error, %s\n", err)
		}
		// Validate delete
		var result map[string]interface{}
		cursor, _ := r.DB(writerTestData.DB).Table(dt.table).Get(dt.id).Run(defaultSession.session)
		cursor.One(&result)
		cursor.Close()
		if result != nil {
			t.Errorf("unexpected result returned, expected nil, got %+v\n", result)
		}

		countCursor, err := r.DB(writerTestData.DB).Table(dt.table).Count().Run(defaultSession.session)
		if err != nil {
			t.Errorf("unable to determine table count, %s\n", err)
		}
		var count int
		countCursor.One(&count)
		countCursor.Close()
		if count != 0 {
			t.Errorf("mismatched doc count, expected 0, got %d\n", count)
		}
	}
}
