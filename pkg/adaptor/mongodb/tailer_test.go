package mongodb

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/message"

	"gopkg.in/mgo.v2/bson"
)

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

	// test that the initial read works against multiple collections
	if err := insertMockTailData(defaultSession, "blah"); err != nil {
		t.Fatalf("unexpected insertMockTailData error, %s\n", err)
	}
	if err := insertMockTailData(defaultSession, "boo"); err != nil {
		t.Fatalf("unexpected insertMockTailData error, %s\n", err)
	}

	tail := newTailer(tailTestData.DB)

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
	msgChan, err := tailFunc(defaultSession, done)
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

func checkCount(desc string, expected int, msgChan <-chan message.Msg, t *testing.T) {
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
