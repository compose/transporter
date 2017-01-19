package mongodb

import (
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/pkg/message"
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

func checkBulkCount(c string, countQuery bson.M, expectedCount int, t *testing.T) {
	count, err := defaultSession.mgoSession.DB(bulkTestData.DB).C(c).Find(countQuery).Count()
	if err != nil {
		t.Errorf("[%s] unable to determine collection count, %s\n", c, err)
	} else if count != expectedCount {
		t.Errorf("[%s] mismatched doc count, expected %d, got %d\n", c, expectedCount, count)
	}
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
			b.Write(message.From(bt.op, ns, data))(defaultSession)
		}
		time.Sleep(3 * time.Second)
		checkBulkCount(bulkTestData.C, bt.countQuery, bt.expectedCount, t)
	}
	close(done)
}

func TestBulkWriteMixedOps(t *testing.T) {
	var wg sync.WaitGroup
	done := make(chan struct{})
	b := newBulker(done, &wg)

	mixedModeC := "mixed_mode"
	ns := fmt.Sprintf("%s.%s", bulkTestData.DB, mixedModeC)
	b.Write(message.From(ops.Insert, ns, map[string]interface{}{"_id": 0}))(defaultSession)
	b.Write(message.From(ops.Insert, ns, map[string]interface{}{"_id": 1}))(defaultSession)
	b.Write(message.From(ops.Insert, ns, map[string]interface{}{"_id": 2}))(defaultSession)
	b.Write(message.From(ops.Update, ns, map[string]interface{}{"_id": 2, "hello": "world"}))(defaultSession)
	b.Write(message.From(ops.Insert, ns, map[string]interface{}{"_id": 3}))(defaultSession)
	b.Write(message.From(ops.Update, ns, map[string]interface{}{"_id": 1, "moar": "tests"}))(defaultSession)
	b.Write(message.From(ops.Insert, ns, map[string]interface{}{"_id": 4, "say": "goodbye"}))(defaultSession)
	b.Write(message.From(ops.Delete, ns, map[string]interface{}{"_id": 1, "moar": "tests"}))(defaultSession)
	b.Write(message.From(ops.Delete, ns, map[string]interface{}{"_id": 3}))(defaultSession)
	b.Write(message.From(ops.Insert, ns, map[string]interface{}{"_id": 5}))(defaultSession)

	// so... after the ops get flushed we should have the following:
	// 4 docs left
	// _id: 2 should have been updated
	time.Sleep(3 * time.Second)
	checkBulkCount(mixedModeC, bson.M{}, 4, t)
	checkBulkCount(mixedModeC, bson.M{"_id": 2, "hello": "world"}, 1, t)
	close(done)
}

func TestBulkOpCount(t *testing.T) {
	var wg sync.WaitGroup
	done := make(chan struct{})
	b := newBulker(done, &wg)

	ns := fmt.Sprintf("%s.%s", bulkTestData.DB, "bar")
	for i := 0; i < maxObjSize; i++ {
		msg := message.From(ops.Insert, fmt.Sprintf("%s.%s", bulkTestData.DB, "bar"), map[string]interface{}{"i": i})
		b.Write(msg)(defaultSession)
	}
	close(done)
	wg.Wait()
	checkBulkCount("bar", bson.M{}, maxObjSize, t)
}

func TestFlushOnDone(t *testing.T) {
	var wg sync.WaitGroup
	done := make(chan struct{})
	b := newBulker(done, &wg)

	ns := fmt.Sprintf("%s.%s", bulkTestData.DB, "baz")
	for i := 0; i < testBulkMsgCount; i++ {
		msg := message.From(ops.Insert, fmt.Sprintf("%s.%s", bulkTestData.DB, "baz"), map[string]interface{}{"i": i})
		b.Write(msg)(defaultSession)
	}
	close(done)
	wg.Wait()
	checkBulkCount("baz", bson.M{}, testBulkMsgCount, t)
}

func TestBulkMulitpleCollections(t *testing.T) {
	var wg sync.WaitGroup
	done := make(chan struct{})
	b := newBulker(done, &wg)

	ns1 := fmt.Sprintf("%s.%s", bulkTestData.DB, "multi_a")
	ns2 := fmt.Sprintf("%s.%s", bulkTestData.DB, "multi_b")
	ns3 := fmt.Sprintf("%s.%s", bulkTestData.DB, "multi_c")
	for i := 0; i < (maxObjSize + 1); i++ {
		b.Write(From(ops.Insert, ns3, map[string]interface{}{"i": i}))(defaultSession)
	}
	for i := 0; i < testBulkMsgCount; i++ {
		b.Write(From(ops.Insert, ns1, map[string]interface{}{"i": i}))(defaultSession)
		b.Write(From(ops.Insert, ns2, map[string]interface{}{"i": i}))(defaultSession)
	}
	checkBulkCount("multi_a", bson.M{}, 0, t)
	checkBulkCount("multi_b", bson.M{}, 0, t)
	checkBulkCount("multi_c", bson.M{}, maxObjSize, t)
	time.Sleep(3 * time.Second)
	checkBulkCount("multi_a", bson.M{}, testBulkMsgCount, t)
	checkBulkCount("multi_b", bson.M{}, testBulkMsgCount, t)
	checkBulkCount("multi_c", bson.M{}, (maxObjSize + 1), t)
}

func TestBulkSize(t *testing.T) {
	b := &Bulk{
		bulkMap: make(map[string]*bulkOperation),
		RWMutex: &sync.RWMutex{},
	}
	ns := fmt.Sprintf("%s.%s", bulkTestData.DB, "size")
	var bsonSize int
	for i := 0; i < (maxObjSize - 1); i++ {
		doc := map[string]interface{}{"i": randStr(2), "rand": randStr(16)}

		bs, err := bson.Marshal(doc)
		if err != nil {
			t.Fatalf("unable to marshal doc to bson, %s", err)
		}
		bsonSize += (len(bs) + 4)

		msg := From(ops.Insert, ns, doc)
		b.Write(msg)(defaultSession)
	}
	bOp := b.bulkMap["size"]
	if int(bOp.bsonOpSize) != bsonSize {
		t.Errorf("mismatched op size, expected %d, got %d\n", bsonSize, int(bOp.bsonOpSize))
	}
}

func randStr(strSize int) string {
	var dictionary = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

	var bytes = make([]byte, strSize)
	rand.Read(bytes)
	for k, v := range bytes {
		bytes[k] = dictionary[v%byte(len(dictionary))]
	}
	return string(bytes)
}
