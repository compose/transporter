package mongodb

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	readerTestData          = &TestData{"reader_test", "foo", 10}
	filteredReaderTestData  = &TestData{"filtered_reader_test", "foo", 10}
	cancelledReaderTestData = &TestData{"cancelled_reader_test", "foo", 100}
)

var filterFunc = func(c string) bool {
	if strings.HasPrefix(c, "system.") {
		return false
	}
	return true
}

func TestRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Read in short mode")
	}

	reader := newReader(false, DefaultCollectionFilter, DefaultUnwind)
	readFunc := reader.Read(filterFunc)
	done := make(chan struct{})
	c, _ := NewClient(WithURI(fmt.Sprintf("mongodb://127.0.0.1:27017/%s", readerTestData.DB)))
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to initialize connection to mongodb, %s", err)
	}
	defer s.(*Session).Close()
	msgChan, err := readFunc(s, done)
	if err != nil {
		t.Fatalf("unexpected Read error, %s\n", err)
	}
	var numMsgs int
	for _ = range msgChan {
		numMsgs++
	}
	if numMsgs != readerTestData.InsertCount {
		t.Errorf("bad message count, expected %d, got %d\n", readerTestData.InsertCount, numMsgs)
	}
	close(done)
}

func TestFilteredRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestFilteredRead in short mode")
	}

	reader := newReader(
		false,
		map[string]CollectionFilter{"foo": CollectionFilter{"i": map[string]interface{}{"$gt": filteredReaderTestData.InsertCount}}},
		DefaultUnwind,
	)

	for i := filteredReaderTestData.InsertCount; i <= 100; i++ {
		defaultSession.mgoSession.DB(filteredReaderTestData.DB).C(filteredReaderTestData.C).Insert(bson.M{"_id": i, "i": i})
	}

	readFunc := reader.Read(filterFunc)
	done := make(chan struct{})
	c, _ := NewClient(WithURI(fmt.Sprintf("mongodb://127.0.0.1:27017/%s", filteredReaderTestData.DB)))
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to initialize connection to mongodb, %s", err)
	}
	defer s.(*Session).Close()
	msgChan, err := readFunc(s, done)
	if err != nil {
		t.Fatalf("unexpected Read error, %s\n", err)
	}
	var numMsgs int
	for _ = range msgChan {
		numMsgs++
	}
	expectedCount := 100 - filteredReaderTestData.InsertCount
	if numMsgs != expectedCount {
		t.Errorf("bad message count, expected %d, got %d\n", expectedCount, numMsgs)
	}
	close(done)
}

func TestCancelledRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestCancelledRead in short mode")
	}

	reader := newReader(false, DefaultCollectionFilter, DefaultUnwind)
	readFunc := reader.Read(filterFunc)
	done := make(chan struct{})
	c, _ := NewClient(WithURI(fmt.Sprintf("mongodb://127.0.0.1:27017/%s", cancelledReaderTestData.DB)))
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to initialize connection to mongodb, %s", err)
	}
	defer s.(*Session).Close()
	msgChan, err := readFunc(s, done)
	if err != nil {
		t.Fatalf("unexpected Read error, %s\n", err)
	}
	go func() {
		time.Sleep(1 * time.Second)
		close(done)
	}()
	var numMsgs int
	for _ = range msgChan {
		time.Sleep(100 * time.Millisecond)
		numMsgs++
	}
	if numMsgs == cancelledReaderTestData.InsertCount {
		t.Errorf("bad message count, expected less than %d but got that", cancelledReaderTestData.InsertCount)
	}
}

func TestReadRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestReadRestart in short mode")
	}

	var db = "restart_read_test"

	c := &Client{
		uri:            fmt.Sprintf("mongodb://127.0.0.1:15000/%s", db),
		sessionTimeout: DefaultSessionTimeout,
		safety:         DefaultSafety,
	}
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to initialize connection to mongodb, %s", err)
	}
	session := s.(*Session)
	session.mgoSession.SetBatch(1)
	session.mgoSession.SetPrefetch(1.0)
	session.mgoSession.SetSocketTimeout(1 * time.Second)

	if dropErr := session.mgoSession.DB(db).DropDatabase(); dropErr != nil {
		log.Errorf("failed to drop database (%s), may affect tests!, %s", db, dropErr)
	}

	for i := 0; i < 100; i++ {
		session.mgoSession.DB(db).C("lotsodata").Insert(bson.M{"i": i})
	}

	reader := newReader(false, DefaultCollectionFilter, DefaultUnwind)
	readFunc := reader.Read(filterFunc)
	done := make(chan struct{})
	msgChan, err := readFunc(s, done)
	if err != nil {
		t.Fatalf("unexpected Read error, %s\n", err)
	}
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(2 * time.Second):
				req, _ := http.NewRequest(
					http.MethodPost,
					"http://127.0.0.1:20000/v1/servers/reader_restart",
					strings.NewReader(`{"action":"restart"}`),
				)
				_, err := http.DefaultClient.Do(req)
				if err != nil {
					log.Errorf("unable to restart server, %s", err)
				}
			}
		}
	}()

	var numMsgs int
	for _ = range msgChan {
		time.Sleep(100 * time.Millisecond)
		numMsgs++
	}
	if numMsgs != 100 {
		t.Errorf("bad message count, expected %d, got %d\n", 100, numMsgs)
	}
	close(done)
}

var (
	tailTestData = &TestData{"tail_test", "foo", 50}
)

func insertMockTailData(s *Session, c string) error {
	for i := 0; i < tailTestData.InsertCount; i++ {
		s.mgoSession.DB(tailTestData.DB).C(c).Insert(bson.M{"i": i})
	}
	return nil
}

func insertUpdateData(s *Session) error {
	if err := s.mgoSession.DB(tailTestData.DB).C("bar").Insert(bson.M{"_id": 0, "hello": "world"}); err != nil {
		return err
	}
	if err := s.mgoSession.DB(tailTestData.DB).C("bar").Update(bson.M{"_id": 0}, bson.M{"$set": bson.M{"hello": "goodbye"}}); err != nil {
		return err
	}
	return nil
}

func insertDeleteData(s *Session) error {
	if err := s.mgoSession.DB(tailTestData.DB).C("baz").Insert(bson.M{"_id": 0, "hello": "world"}); err != nil {
		return err
	}
	if err := s.mgoSession.DB(tailTestData.DB).C("baz").RemoveId(0); err != nil {
		return err
	}
	return nil
}

func TestTail(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Tail in short mode")
	}

	defaultSession.mgoSession.DB(tailTestData.DB).C("bar").Create(&mgo.CollectionInfo{})
	defaultSession.mgoSession.DB(tailTestData.DB).C("baz").Create(&mgo.CollectionInfo{})

	// test that the initial read works against multiple collections
	if err := insertMockTailData(defaultSession, "blah"); err != nil {
		t.Fatalf("unexpected insertMockTailData error, %s\n", err)
	}
	if err := insertMockTailData(defaultSession, "boo"); err != nil {
		t.Fatalf("unexpected insertMockTailData error, %s\n", err)
	}

	tail := newReader(true, DefaultCollectionFilter, DefaultUnwind)

	time.Sleep(1 * time.Second)
	tailFunc := tail.Read(func(c string) bool {
		if strings.HasPrefix(c, "system.") {
			return false
		} else if c == "skip" {
			return false
		}
		return true

	})
	done := make(chan struct{})
	c, _ := NewClient(WithURI(fmt.Sprintf("mongodb://127.0.0.1:27017/%s", tailTestData.DB)))
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to initialize connection to mongodb, %s", err)
	}
	defer s.(*Session).Close()
	msgChan, err := tailFunc(s, done)
	if err != nil {
		t.Fatalf("unexpected Tail error, %s\n", err)
	}
	// drain messages inserted from all 3 collections (foo, blah, boo)
	checkCount("initial drain", 3*tailTestData.InsertCount, msgChan, t)
	// let the iterator timeout before next insert
	time.Sleep(6 * time.Second)
	if err = insertMockTailData(defaultSession, "foo"); err != nil {
		t.Fatalf("unexpected insertMockTailData error, %s\n", err)
	}
	checkCount("oplogTimeout", tailTestData.InsertCount, msgChan, t)

	if err := insertUpdateData(defaultSession); err != nil {
		t.Fatalf("unexpected insertUpdateData error, %s\n", err)
	}
	checkCount("insertUpdateData", 2, msgChan, t)

	if err := insertDeleteData(defaultSession); err != nil {
		t.Fatalf("unexpected insertDeleteData error, %s\n", err)
	}
	checkCount("insertDeleteData", 2, msgChan, t)

	if err := defaultSession.mgoSession.DB(tailTestData.DB).C("skip").Insert(bson.M{}); err != nil {
		t.Fatalf("unexpected Insert error, %s\n", err)
	}
	checkCount("skip", 0, msgChan, t)
	close(done)
}

func checkCount(desc string, expected int, msgChan <-chan client.MessageSet, t *testing.T) {
	var numMsgs int
	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		for {
			select {
			case <-msgChan:
				numMsgs++
			case <-time.After(1 * time.Second):
				wg.Done()
				return
			}
		}
	}(&wg)
	wg.Wait()
	if numMsgs != expected {
		t.Errorf("[%s] bad message count, expected %d, got %d\n", desc, expected, numMsgs)
	}
}

var sorttests = []struct {
	in       interface{}
	sortable bool
}{
	{"IamAstring", true},
	{bson.NewObjectId(), true},
	{time.Now(), true},
	{int64(1000), true},
	{float64(100.9), true},
	{100.5, true},
	{1000, false},
	{false, false},
}

func TestSortable(t *testing.T) {
	for _, st := range sorttests {
		out := sortable(st.in)
		if out != st.sortable {
			t.Errorf("unexpected result for %+v, expected %+v, got %+v", st.in, st.sortable, out)
		}
	}
}
