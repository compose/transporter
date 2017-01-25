package mongodb

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/log"
	"gopkg.in/mgo.v2/bson"
)

var (
	readerTestData = &TestData{"reader_test", "foo", 10}
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

	reader := newReader(readerTestData.DB)
	readFunc := reader.Read(filterFunc)
	done := make(chan struct{})
	msgChan, err := readFunc(defaultSession, done)
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

func TestReadRestart(t *testing.T) {
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

	reader := newReader(db)
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
