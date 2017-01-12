package mongodb

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/pkg/message/ops"
)

var (
	bulkTestData     = &TestData{"bulk_test", "foo", 0}
	testBulkMsgCount = 10
	bulkTests        = []*BulkTest{
		&BulkTest{ops.Insert, bson.M{}, testBulkMsgCount, nil},
		&BulkTest{ops.Update, bson.M{"hello": "world"}, testBulkMsgCount, map[string]interface{}{"hello": "world"}},
		&BulkTest{ops.Delete, bson.M{}, 0, nil},
	}
)

type BulkTest struct {
	op            ops.Op
	countQuery    bson.M
	expectedCount int
	extraData     map[string]interface{}
}

func TestBulkWrite(t *testing.T) {
	var wg sync.WaitGroup
	done := make(chan struct{})
	b := newBulker(done, &wg)

	ns := fmt.Sprintf("%s.%s", bulkTestData.DB, bulkTestData.C)
	for _, bt := range bulkTests {
		for i := 0; i < testBulkMsgCount; i++ {
			data := map[string]interface{}{"_id": i, "i": i}
			for k, v := range bt.extraData {
				data[k] = v
			}
			b.Write(From(bt.op, ns, data))(defaultSession)
		}
		time.Sleep(3 * time.Second)
		count, err := defaultSession.mgoSession.DB(bulkTestData.DB).C(bulkTestData.C).Find(bt.countQuery).Count()
		if err != nil {
			t.Errorf("[%s] unable to determine collection count, %s\n", bulkTestData.C, err)
		} else if count != bt.expectedCount {
			t.Errorf("[%s] mismatched doc count, expected %d, got %d\n", bulkTestData.C, bt.expectedCount, count)
		}
	}
}

func TestBulkWriteMixedOps(t *testing.T) {
	var wg sync.WaitGroup
	done := make(chan struct{})
	b := newBulker(done, &wg)

	mixedModeC := "mixed_mode"
	ns := fmt.Sprintf("%s.%s", bulkTestData.DB, mixedModeC)
	b.Write(From(ops.Insert, ns, map[string]interface{}{"_id": 0}))(defaultSession)
	b.Write(From(ops.Insert, ns, map[string]interface{}{"_id": 1}))(defaultSession)
	b.Write(From(ops.Insert, ns, map[string]interface{}{"_id": 2}))(defaultSession)
	b.Write(From(ops.Update, ns, map[string]interface{}{"_id": 2, "hello": "world"}))(defaultSession)
	b.Write(From(ops.Insert, ns, map[string]interface{}{"_id": 3}))(defaultSession)
	b.Write(From(ops.Update, ns, map[string]interface{}{"_id": 1, "moar": "tests"}))(defaultSession)
	b.Write(From(ops.Insert, ns, map[string]interface{}{"_id": 4, "say": "goodbye"}))(defaultSession)
	b.Write(From(ops.Delete, ns, map[string]interface{}{"_id": 1, "moar": "tests"}))(defaultSession)
	b.Write(From(ops.Delete, ns, map[string]interface{}{"_id": 3}))(defaultSession)
	b.Write(From(ops.Insert, ns, map[string]interface{}{"_id": 5}))(defaultSession)

	// so... after the ops get flushed we should have the following:
	// 4 docs left
	// _id: 2 should have been updated
	time.Sleep(3 * time.Second)
	count, err := defaultSession.mgoSession.DB(bulkTestData.DB).C(mixedModeC).Find(bson.M{}).Count()
	if err != nil {
		t.Errorf("[%s] unable to determine collection count, %s\n", mixedModeC, err)
	} else if count != 4 {
		t.Errorf("[%s] mismatched doc count, expected %d, got %d\n", mixedModeC, 4, count)
	}
	var result bson.M
	defaultSession.mgoSession.DB(bulkTestData.DB).C(mixedModeC).Find(bson.M{"_id": 2}).One(&result)
	if !reflect.DeepEqual(result, bson.M{"_id": 2, "hello": "world"}) {
		t.Errorf("[%s] mismatched doc, expected %+v, got %+v\n", mixedModeC, bson.M{"_id": 2, "hello": "world"}, result)
	}
}

func TestBulkOpCount(t *testing.T) {
	var wg sync.WaitGroup
	done := make(chan struct{})
	b := newBulker(done, &wg)

	for i := 0; i < 1000; i++ {
		msg := From(ops.Insert, fmt.Sprintf("%s.%s", bulkTestData.DB, "bar"), map[string]interface{}{"i": i})
		b.Write(msg)(defaultSession)
	}
	count, err := defaultSession.mgoSession.DB(bulkTestData.DB).C("bar").Count()
	if err != nil {
		t.Errorf("[bar] unable to determine collection count, %s\n", err)
	} else if count != 1000 {
		t.Errorf("[bar] mismatched doc count, expected %d, got %d\n", 1000, count)
	}
	close(done)
	wg.Wait()
}

func TestFlushOnDone(t *testing.T) {
	var wg sync.WaitGroup
	done := make(chan struct{})
	b := newBulker(done, &wg)

	for i := 0; i < testBulkMsgCount; i++ {
		msg := From(ops.Insert, fmt.Sprintf("%s.%s", bulkTestData.DB, "baz"), map[string]interface{}{"i": i})
		b.Write(msg)(defaultSession)
	}
	close(done)
	wg.Wait()
	time.Sleep(1 * time.Second)
	count, err := defaultSession.mgoSession.DB(bulkTestData.DB).C("baz").Count()
	if err != nil {
		t.Errorf("[baz] unable to determine collection count, %s\n", err)
	} else if count != testBulkMsgCount {
		t.Errorf("[baz] mismatched doc count, expected %d, got %d\n", testBulkMsgCount, count)
	}
}
