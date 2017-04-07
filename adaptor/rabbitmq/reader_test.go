package rabbitmq

import (
	"sync"
	"testing"
	"time"

	"github.com/compose/transporter/client"
	"github.com/streadway/amqp"
)

var (
	readerTestData    = TestData{"reader_queue", "reader_key", 10}
	readerBadDataTest = TestData{"reader_bad_data_queue", "reader_bad_data_key", 10}
)

func TestRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Read in short mode")
	}
	reader := &Reader{DefaultURI, DefaultAPIPort}
	readFunc := reader.Read(map[string]client.MessageSet{},
		func(queue string) bool { return queue == readerTestData.Queue })
	done := make(chan struct{})
	msgChan, err := readFunc(defaultSession, done)
	if err != nil {
		t.Fatalf("unexpected Read error, %s\n", err)
	}
	checkCount(readerTestData.Queue, readerTestData.InsertCount, msgChan, t)
	close(done)
}

func TestReadBadData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Read in short mode")
	}
	reader := &Reader{DefaultURI, DefaultAPIPort}
	readFunc := reader.Read(map[string]client.MessageSet{},
		func(queue string) bool { return queue == readerBadDataTest.Queue })
	done := make(chan struct{})
	msgChan, err := readFunc(defaultSession, done)
	if err != nil {
		t.Fatalf("unexpected Read error, %s\n", err)
	}
	msg := amqp.Publishing{
		DeliveryMode: amqp.Transient,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         []byte(`{"id": %d, "message": "hello"`),
	}
	if err := defaultSession.channel.Publish(testExchange, readerBadDataTest.RoutingKey, false, false, msg); err != nil {
		t.Fatalf("failed to publish to queue (%s), %s", readerBadDataTest.Queue, err)
	}
	checkCount(readerBadDataTest.Queue, readerBadDataTest.InsertCount, msgChan, t)
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
