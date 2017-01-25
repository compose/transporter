package mongodb

import (
	"fmt"
	"reflect"
	"testing"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
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
	w := newWriter()
	for _, ot := range optests {
		if _, ok := w.writeMap[ot.op]; ok != ot.registered {
			t.Errorf("op (%s) registration incorrect, expected %+v, got %+v\n", ot.op.String(), ot.registered, ok)
		}
	}
}

var (
	writerTestData = &TestData{"writer_test", "test", 0}

	inserttests = []struct {
		collection       string
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
				map[string]interface{}{"_id": 0, "hello": "world"},
				map[string]interface{}{"_id": 1, "bonjour": "world"},
				map[string]interface{}{"_id": 2, "hola": "world"},
				map[string]interface{}{"_id": 3, "guten tag": "world"},
				map[string]interface{}{"_id": 0, "hello": "moar world"},
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
	w := newWriter()
	for _, it := range inserttests {
		for _, data := range it.data {
			msg := message.From(ops.Insert, fmt.Sprintf("%s.%s", writerTestData.DB, it.collection), data)
			if err := w.Write(msg)(defaultSession); err != nil {
				t.Errorf("unexpected Insert error, %s\n", err)
			}
		}
		count, err := defaultSession.mgoSession.DB(writerTestData.DB).C(it.collection).Count()
		if err != nil {
			t.Errorf("unable to determine collection count, %s\n", err)
		} else if count != it.docCount {
			t.Errorf("mismatched doc count, expected %d, got %d\n", it.docCount, count)
		}
		if it.verifyLastInsert {
			var result interface{}
			lastDoc := it.data[len(it.data)-1]
			expectedDoc := bson.M{"_id": lastDoc.Get("_id")}
			for k, v := range lastDoc {
				expectedDoc[k] = v
			}
			if err := defaultSession.mgoSession.DB(writerTestData.DB).C(it.collection).FindId(lastDoc.Get("_id")).One(&result); err != nil {
				t.Fatalf("unexpected FindId error, %s\n", err)
			}
			if !reflect.DeepEqual(expectedDoc, result) {
				t.Errorf("mismatched document, expected %+v, got %+v\n", expectedDoc, result)
			}
		}
	}
}

var updatetests = []struct {
	collection  string
	id          bson.ObjectId
	originalDoc data.Data
	updatedDoc  data.Data
}{
	{
		"updatesimple",
		bson.NewObjectId(),
		map[string]interface{}{"hello": "world"},
		map[string]interface{}{"hello": "again"},
	},
}

func TestUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Update in short mode")
	}
	w := newWriter()
	for _, ut := range updatetests {
		ns := fmt.Sprintf("%s.%s", writerTestData.DB, ut.collection)
		// Insert data
		ut.originalDoc.Set("_id", ut.id)
		msg := message.From(ops.Insert, ns, ut.originalDoc)
		if err := w.Write(msg)(defaultSession); err != nil {
			t.Errorf("unexpected Insert error, %s\n", err)
		}
		// Update data
		ut.updatedDoc.Set("_id", ut.id)
		msg = message.From(ops.Update, ns, ut.updatedDoc)
		if err := w.Write(msg)(defaultSession); err != nil {
			t.Errorf("unexpected Update error, %s\n", err)
		}
		// Validate update
		expectedDoc := bson.M{"_id": ut.id}
		for k, v := range ut.updatedDoc {
			expectedDoc[k] = v
		}
		var result interface{}
		if err := defaultSession.mgoSession.DB(writerTestData.DB).C(ut.collection).FindId(ut.id).One(&result); err != nil {
			t.Fatalf("unexpected FindId error, %s\n", err)
		}
		if !reflect.DeepEqual(expectedDoc, result) {
			t.Errorf("mismatched document, expected %+v(%T), got %+v(%T)\n", expectedDoc, expectedDoc, result, result)
		}
	}
}

var deletetests = []struct {
	collection  string
	id          bson.ObjectId
	originalDoc data.Data
}{
	{
		"deletesimple",
		bson.NewObjectId(),
		map[string]interface{}{"hello": "world"},
	},
}

func TestDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Update in short mode")
	}
	w := newWriter()
	for _, dt := range deletetests {
		ns := fmt.Sprintf("%s.%s", writerTestData.DB, dt.collection)
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
		var result interface{}
		if err := defaultSession.mgoSession.DB(writerTestData.DB).C(dt.collection).FindId(dt.id).One(&result); err != mgo.ErrNotFound {
			t.Errorf("unexpected error returned, expected mgo.ErrorNotFound, got %T\n", err)
		}
	}
}
