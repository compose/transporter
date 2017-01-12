package mongodb

import (
	"strings"
	"testing"
)

var (
	readerTestData = &TestData{"reader_test", "foo", 10}
)

func TestRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Read in short mode")
	}

	reader := newReader(readerTestData.DB)
	readFunc := reader.Read(func(c string) bool {
		if strings.HasPrefix(c, "system.") {
			return false
		}
		return true
	})
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
