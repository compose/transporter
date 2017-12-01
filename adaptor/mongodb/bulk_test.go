package mongodb

import (
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

var (
	bulkTestData     = &TestData{"bulk_test", "foo", 0}
	testBulkMsgCount = 20
	bulkTests        = []*BulkTest{
		&BulkTest{ops.Insert, bson.M{}, testBulkMsgCount, nil},
		&BulkTest{ops.Update, bson.M{"hello": "world"}, testBulkMsgCount, map[string]interface{}{"hello": "world"}},
		&BulkTest{ops.Delete, bson.M{}, 0, nil},
		&BulkTest{ops.Insert, bson.M{}, testBulkMsgCount, map[string]interface{}{"requestTooLarge": randStr(5e6)}},
		&BulkTest{ops.Delete, bson.M{}, 0, nil},
		&BulkTest{ops.Insert, bson.M{}, testBulkMsgCount, map[string]interface{}{"bulkTooLarge": randStr(1e6)}},
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
	confirms, cleanup := adaptor.MockConfirmWrites()
	defer adaptor.VerifyWriteConfirmed(cleanup, t)
	var wg sync.WaitGroup
	done := make(chan struct{})
	b := newBulker(done, &wg)

	c, _ := NewClient(WithURI(fmt.Sprintf("mongodb://127.0.0.1:27017/%s", bulkTestData.DB)))
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to initialize connection to mongodb, %s", err)
	}
	defer s.(*Session).Close()
	for _, bt := range bulkTests {
		for i := 0; i < testBulkMsgCount; i++ {
			data := map[string]interface{}{"_id": i, "i": i}
			for k, v := range bt.extraData {
				data[k] = v
			}
			b.Write(message.WithConfirms(confirms, message.From(bt.op, bulkTestData.C, data)))(s)
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

	c, _ := NewClient(WithURI(fmt.Sprintf("mongodb://127.0.0.1:27017/%s", bulkTestData.DB)))
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to initialize connection to mongodb, %s", err)
	}
	defer s.(*Session).Close()
	mixedModeC := "mixed_mode"
	b.Write(message.From(ops.Insert, mixedModeC, map[string]interface{}{"_id": 0}))(s)
	b.Write(message.From(ops.Insert, mixedModeC, map[string]interface{}{"_id": 1}))(s)
	b.Write(message.From(ops.Insert, mixedModeC, map[string]interface{}{"_id": 2}))(s)
	b.Write(message.From(ops.Update, mixedModeC, map[string]interface{}{"_id": 2, "hello": "world"}))(s)
	b.Write(message.From(ops.Insert, mixedModeC, map[string]interface{}{"_id": 3}))(s)
	b.Write(message.From(ops.Update, mixedModeC, map[string]interface{}{"_id": 1, "moar": "tests"}))(s)
	b.Write(message.From(ops.Insert, mixedModeC, map[string]interface{}{"_id": 4, "say": "goodbye"}))(s)
	b.Write(message.From(ops.Delete, mixedModeC, map[string]interface{}{"_id": 1, "moar": "tests"}))(s)
	b.Write(message.From(ops.Delete, mixedModeC, map[string]interface{}{"_id": 3}))(s)
	b.Write(message.From(ops.Insert, mixedModeC, map[string]interface{}{"_id": 5}))(s)

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

	c, _ := NewClient(WithURI(fmt.Sprintf("mongodb://127.0.0.1:27017/%s", bulkTestData.DB)))
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to initialize connection to mongodb, %s", err)
	}
	defer s.(*Session).Close()
	for i := 0; i < maxObjSize; i++ {
		b.Write(message.From(ops.Insert, "bar", map[string]interface{}{"i": i}))(s)
	}
	close(done)
	wg.Wait()
	checkBulkCount("bar", bson.M{}, maxObjSize, t)
}

func TestBulkIsDup(t *testing.T) {
	confirms, cleanup := adaptor.MockConfirmWrites()
	defer adaptor.VerifyWriteConfirmed(cleanup, t)
	var wg sync.WaitGroup
	done := make(chan struct{})
	b := newBulker(done, &wg)

	c, _ := NewClient(WithURI(fmt.Sprintf("mongodb://127.0.0.1:27017/%s", bulkTestData.DB)))
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to initialize connection to mongodb, %s", err)
	}
	defer s.(*Session).Close()
	for i := 0; i < testBulkMsgCount; i++ {
		b.Write(
			message.WithConfirms(
				confirms,
				message.From(ops.Insert, "dupErr", map[string]interface{}{"_id": i, "i": i}),
			),
		)(s)
	}
	time.Sleep(3 * time.Second)
	checkBulkCount("dupErr", bson.M{}, testBulkMsgCount, t)

	for i := 0; i < (2 * testBulkMsgCount); i++ {
		b.Write(
			message.WithConfirms(
				confirms,
				message.From(ops.Insert, "dupErr", map[string]interface{}{"_id": i, "i": i}),
			),
		)(s)
	}

	close(done)
	wg.Wait()
	checkBulkCount("dupErr", bson.M{}, (2 * testBulkMsgCount), t)
}

func TestFlushOnDone(t *testing.T) {
	var wg sync.WaitGroup
	done := make(chan struct{})
	b := newBulker(done, &wg)

	c, _ := NewClient(WithURI(fmt.Sprintf("mongodb://127.0.0.1:27017/%s", bulkTestData.DB)))
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to initialize connection to mongodb, %s", err)
	}
	defer s.(*Session).Close()
	for i := 0; i < testBulkMsgCount; i++ {
		b.Write(message.From(ops.Insert, "baz", map[string]interface{}{"i": i}))(s)
	}
	close(done)
	wg.Wait()
	checkBulkCount("baz", bson.M{}, testBulkMsgCount, t)
}

func TestBulkMulitpleCollections(t *testing.T) {
	var wg sync.WaitGroup
	done := make(chan struct{})
	b := newBulker(done, &wg)

	c, _ := NewClient(WithURI(fmt.Sprintf("mongodb://127.0.0.1:27017/%s", bulkTestData.DB)))
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to initialize connection to mongodb, %s", err)
	}
	defer s.(*Session).Close()
	for i := 0; i < (maxObjSize + 1); i++ {
		b.Write(message.From(ops.Insert, "multi_c", map[string]interface{}{"i": i}))(s)
	}
	for i := 0; i < testBulkMsgCount; i++ {
		b.Write(message.From(ops.Insert, "multi_a", map[string]interface{}{"i": i}))(s)
		b.Write(message.From(ops.Insert, "multi_b", map[string]interface{}{"i": i}))(s)
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

	c, _ := NewClient(WithURI(fmt.Sprintf("mongodb://127.0.0.1:27017/%s", bulkTestData.DB)))
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to initialize connection to mongodb, %s", err)
	}
	defer s.(*Session).Close()

	var bsonSize int
	for i := 0; i < (maxObjSize - 1); i++ {
		doc := map[string]interface{}{"i": randStr(2), "rand": randStr(16)}

		bs, err := bson.Marshal(doc)
		if err != nil {
			t.Fatalf("unable to marshal doc to bson, %s", err)
		}
		bsonSize += (len(bs) + 4)

		b.Write(message.From(ops.Insert, "size", doc))(s)
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
