package rabbitmq

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
	"github.com/streadway/amqp"
)

var (
	writerTestData  = TestData{"writer_queue", "writer_key", 0}
	writerTestData2 = TestData{"writer_queue_2", "writer_key_2", 0}
	writerTestData3 = TestData{"writer_queue_3", "", 0}
)

func TestWriteWithKeyInField(t *testing.T) {
	w := &Writer{amqp.Transient, "my_key", true}
	for i := 0; i < 10; i++ {
		_, err := w.Write(
			message.From(
				ops.Insert,
				testExchange,
				map[string]interface{}{"my_key": writerTestData.RoutingKey, "i": i},
			),
		)(defaultSession)
		if err != nil {
			t.Fatalf("unexpected Write error, %s\n", err)
		}
	}
	_, err := w.Write(
		message.From(
			ops.Delete,
			testExchange,
			map[string]interface{}{"my_key": writerTestData.RoutingKey, "i": 100},
		),
	)(defaultSession)
	if err != nil {
		t.Fatalf("unexpected Write error, %s\n", err)
	}
	checkQueueCount(writerTestData.Queue, 10, t)
}

func TestWriteWithStaticKey(t *testing.T) {
	w := &Writer{amqp.Transient, writerTestData2.RoutingKey, false}
	for i := 0; i < 10; i++ {
		_, err := w.Write(
			message.From(
				ops.Insert,
				testExchange,
				map[string]interface{}{"i": i},
			),
		)(defaultSession)
		if err != nil {
			t.Fatalf("unexpected Write error, %s\n", err)
		}
	}
	checkQueueCount(writerTestData2.Queue, 10, t)
}

func TestWriteWithEmptyKey(t *testing.T) {
	w := &Writer{amqp.Transient, writerTestData3.RoutingKey, false}
	for i := 0; i < 10; i++ {
		_, err := w.Write(
			message.From(
				ops.Insert,
				testExchange,
				map[string]interface{}{"i": i},
			),
		)(defaultSession)
		if err != nil {
			t.Fatalf("unexpected Write error, %s\n", err)
		}
	}
	checkQueueCount(writerTestData3.Queue, 10, t)
}

func checkQueueCount(queue string, count int, t *testing.T) {
	time.Sleep(5 * time.Second)
	u, _ := url.Parse(DefaultURI)
	vhost := u.Path
	if vhost != "/" {
		vhost = vhost[1:]
	}
	apiURL := fmt.Sprintf(
		"http://%s:%d/api/queues/%s/%s",
		u.Hostname(), DefaultAPIPort, url.QueryEscape(vhost), queue,
	)
	log.With("apiURL", apiURL).Infoln("requesting queue info")
	req, _ := http.NewRequest(http.MethodGet, apiURL, nil)
	if u.User != nil {
		if pwd, ok := u.User.Password(); ok {
			req.SetBasicAuth(u.User.Username(), pwd)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected http error, %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.StatusCode)
	}
	defer resp.Body.Close()
	var queueInfo struct {
		MessagesReady float64 `json:"messages_ready"`
	}
	json.NewDecoder(resp.Body).Decode(&queueInfo)
	if queueInfo.MessagesReady != 10 {
		t.Errorf("wrong message count, expected 10, got %f", queueInfo.MessagesReady)
	}
}
