package rethinkdb

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"

	r "gopkg.in/gorethink/gorethink.v3"
)

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

func TestBulkInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Insert in short mode")
	}
	var wg sync.WaitGroup
	done := make(chan struct{})
	w := newWriter(writerTestData.DB, done, &wg)

	if _, err := r.DB(writerTestData.DB).TableCreate("bulk").RunWrite(defaultSession.session); err != nil {
		log.Errorf("failed to create table (bulk) in %s, may affect tests!, %s", writerTestData.DB, err)
	}

	for i := 0; i < 999; i++ {
		msg := message.From(ops.Insert, "bulk", map[string]interface{}{"i": i})
		if _, err := w.Write(msg)(defaultSession); err != nil {
			t.Errorf("unexpected Insert error, %s", err)
		}
	}
	time.Sleep(3 * time.Second)
	close(done)
	wg.Wait()
	countResp, err := r.DB(writerTestData.DB).Table("bulk").Count().Run(defaultSession.session)
	if err != nil {
		t.Errorf("unable to determine table count, %s", err)
	}
	var count int
	countResp.One(&count)
	countResp.Close()
	if count != 999 {
		t.Errorf("[bulk] mismatched doc count, expected 999, got %d", count)
	}
}

func TestInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Insert in short mode")
	}

	for _, it := range inserttests {
		var wg sync.WaitGroup
		done := make(chan struct{})
		w := newWriter(writerTestData.DB, done, &wg)

		if _, err := r.DB(writerTestData.DB).TableCreate(it.table).RunWrite(defaultSession.session); err != nil {
			log.Errorf("failed to create table (%s) in %s, may affect tests!, %s", it.table, writerTestData.DB, err)
		}

		for _, data := range it.data {
			msg := message.From(ops.Insert, it.table, data)
			if _, err := w.Write(msg)(defaultSession); err != nil {
				t.Errorf("unexpected Insert error, %s\n", err)
			}
		}
		close(done)
		wg.Wait()
		countResp, err := r.DB(writerTestData.DB).Table(it.table).Count().Run(defaultSession.session)
		if err != nil {
			t.Errorf("unable to determine table count, %s\n", err)
		}
		var count int
		countResp.One(&count)
		countResp.Close()
		if count != it.docCount {
			t.Errorf("[%s] mismatched doc count, expected %d, got %d\n", it.table, it.docCount, count)
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
				t.Errorf("[%s] mismatched document, expected %+v (%T), got %+v (%T)\n", it.table, lastDoc.AsMap(), lastDoc.AsMap(), result, result)
			}
		}
	}
}

var updatetests = []struct {
	table       string
	originalDoc data.Data
	updatedDoc  data.Data
}{
	{
		"updatesimple",
		map[string]interface{}{"id": "4e9e5bc2-9b11-4143-9aa1-75c10e7a193a", "hello": "world"},
		map[string]interface{}{"id": "4e9e5bc2-9b11-4143-9aa1-75c10e7a193a", "hello": "again"},
	},
}

func TestUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Update in short mode")
	}

	for _, ut := range updatetests {
		var wg sync.WaitGroup
		done := make(chan struct{})
		w := newWriter(writerTestData.DB, done, &wg)
		if _, err := r.DB(writerTestData.DB).TableCreate(ut.table).RunWrite(defaultSession.session); err != nil {
			log.Errorf("failed to create table (%s) in %s, may affect tests!, %s", ut.table, writerTestData.DB, err)
		}

		// Insert data
		msg := message.From(ops.Insert, ut.table, ut.originalDoc)
		if _, err := w.Write(msg)(defaultSession); err != nil {
			t.Errorf("unexpected Insert error, %s\n", err)
		}
		// Update data
		msg = message.From(ops.Update, ut.table, ut.updatedDoc)
		if _, err := w.Write(msg)(defaultSession); err != nil {
			t.Errorf("unexpected Update error, %s\n", err)
		}
		close(done)
		wg.Wait()
		// Validate update
		expectedDoc := map[string]interface{}{}
		for k, v := range ut.updatedDoc {
			expectedDoc[k] = v
		}
		var result map[string]interface{}
		cursor, err := r.DB(writerTestData.DB).Table(ut.table).Get(ut.updatedDoc["id"]).Run(defaultSession.session)
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
	for _, dt := range deletetests {
		var wg sync.WaitGroup
		done := make(chan struct{})
		w := newWriter(writerTestData.DB, done, &wg)
		if _, err := r.DB(writerTestData.DB).TableCreate(dt.table).RunWrite(defaultSession.session); err != nil {
			log.Errorf("failed to create table (%s) in %s, may affect tests!, %s", dt.table, writerTestData.DB, err)
		}

		// Insert data
		dt.originalDoc.Set("_id", dt.id)
		msg := message.From(ops.Insert, dt.table, dt.originalDoc)
		if _, err := w.Write(msg)(defaultSession); err != nil {
			t.Errorf("unexpected Insert error, %s\n", err)
		}
		// Delete data
		msg = message.From(ops.Delete, dt.table, dt.originalDoc)
		if _, err := w.Write(msg)(defaultSession); err != nil {
			t.Errorf("unexpected Delete error, %s\n", err)
		}
		close(done)
		wg.Wait()
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
