package rethinkdb

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/compose/transporter/client"

	r "gopkg.in/gorethink/gorethink.v3"
)

var (
	readerTestData = &TestData{"reader_test", "foo", 10}
)

func TestRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Read in short mode")
	}

	reader := newReader(false)
	readFunc := reader.Read(map[string]client.MessageSet{}, func(c string) bool { return true })
	done := make(chan struct{})
	c, err := NewClient(WithURI(fmt.Sprintf("rethinkdb://127.0.0.1:28015/%s", readerTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to rethinkdb, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to rethinkdb, %s", err)
	}
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

var (
	tailTestData   = &TestData{"tail_test", "foo", 50}
	tailTestTables = []string{"bar", "baz", "blah", "boo", "skip"}
)

func creatMockTables(s *Session) error {
	for _, t := range tailTestTables {
		if _, err := r.DB(tailTestData.DB).TableCreate(t).RunWrite(s.session); err != nil {
			return err

		}
	}
	return nil
}

func insertMockTailData(s *Session, c string) error {
	for i := 0; i < tailTestData.InsertCount; i++ {
		r.DB(tailTestData.DB).Table(c).Insert(map[string]interface{}{"i": i}).RunWrite(s.session)
	}
	return nil
}

func insertUpdateData(s *Session) error {
	if _, err := r.DB(tailTestData.DB).Table("bar").Insert(map[string]interface{}{"id": 0, "hello": "world"}).RunWrite(s.session); err != nil {
		return err
	}
	if _, err := r.DB(tailTestData.DB).Table("bar").Insert(map[string]interface{}{"id": 0, "hello": "goodbye"}, r.InsertOpts{Conflict: "replace"}).RunWrite(s.session); err != nil {
		return err
	}
	return nil
}

func insertDeleteData(s *Session) error {
	if _, err := r.DB(tailTestData.DB).Table("baz").Insert(map[string]interface{}{"id": 0, "hello": "world"}).RunWrite(s.session); err != nil {
		return err
	}
	if _, err := r.DB(tailTestData.DB).Table("baz").Get(0).Delete().RunWrite(s.session); err != nil {
		return err
	}
	return nil
}

func TestTail(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Tail in short mode")
	}

	if err := creatMockTables(defaultSession); err != nil {
		t.Fatalf("unexpected creatMockTables error, %s\n", err)
	}

	// test that the initial read works against multiple collections
	if err := insertMockTailData(defaultSession, "blah"); err != nil {
		t.Fatalf("unexpected insertMockTailData error, %s\n", err)
	}
	if err := insertMockTailData(defaultSession, "boo"); err != nil {
		t.Fatalf("unexpected insertMockTailData error, %s\n", err)
	}

	tail := newReader(true)

	time.Sleep(1 * time.Second)
	tailFunc := tail.Read(map[string]client.MessageSet{}, func(c string) bool {
		if c == "skip" {
			return false
		}
		return true
	})
	done := make(chan struct{})
	c, err := NewClient(WithURI(fmt.Sprintf("rethinkdb://127.0.0.1:28015/%s", tailTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to rethinkdb, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to rethinkdb, %s", err)
	}
	msgChan, err := tailFunc(s, done)
	if err != nil {
		t.Fatalf("unexpected Tail error, %s\n", err)
	}
	// drain messages inserted from all 3 tables (foo, blah, boo)
	checkCount("initial drain", 3*tailTestData.InsertCount, msgChan, t)

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

	if _, err := r.DB(tailTestData.DB).Table("skip").Insert(map[string]interface{}{}).RunWrite(defaultSession.session); err != nil {
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
