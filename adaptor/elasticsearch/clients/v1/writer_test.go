package v1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/compose/transporter/adaptor/elasticsearch/clients"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

const (
	defaultURL   = "http://127.0.0.1:9200"
	defaultIndex = "test_v1"
	testType     = "test"
)

var (
	testURL = os.Getenv("ES_V1_URL")
)

func fullURL(suffix string) string {
	return fmt.Sprintf("%s/%s%s", testURL, defaultIndex, suffix)
}

func setup() error {
	log.Debugln("setting up tests")
	return clearTestData()
}

func clearTestData() error {
	req, _ := http.NewRequest(http.MethodDelete, fullURL(""), nil)
	resp, err := http.DefaultClient.Do(req)
	log.Debugf("clearTestData response, %+v", resp)
	return err
}

func TestMain(m *testing.M) {
	if testURL == "" {
		testURL = defaultURL
	}

	if err := setup(); err != nil {
		log.Errorf("unable to setup tests, %s", err)
		os.Exit(1)
	}
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func shutdown() {
	log.Debugln("shutting down tests")
	clearTestData()
	log.Debugln("tests shutdown complete")
}

type countResponse struct {
	Count int `json:"count"`
}

func TestWriter(t *testing.T) {
	opts := &clients.ClientOptions{
		URLs:       []string{testURL},
		HTTPClient: http.DefaultClient,
		Index:      defaultIndex,
	}
	vc := clients.Clients["v1"]
	w, _ := vc.Creator(opts)
	w.Write(
		message.WithConfirms(
			make(chan struct{}),
			message.From(ops.Insert, testType, map[string]interface{}{"hello": "world"})),
	)(nil)
	w.Write(
		message.WithConfirms(
			make(chan struct{}),
			message.From(ops.Insert, testType, map[string]interface{}{"_id": "booya", "hello": "world"})),
	)(nil)
	w.Write(
		message.WithConfirms(
			make(chan struct{}),
			message.From(ops.Update, testType, map[string]interface{}{"_id": "booya", "hello": "goodbye"})),
	)(nil)
	w.Write(
		message.WithConfirms(
			make(chan struct{}),
			message.From(ops.Delete, testType, map[string]interface{}{"_id": "booya", "hello": "goodbye"})),
	)(nil)

	if _, err := http.Get(fullURL("/_refresh")); err != nil {
		t.Fatalf("_refresh request failed, %s", err)
	}
	time.Sleep(1 * time.Second)

	resp, err := http.Get(fullURL("/_count"))
	if err != nil {
		t.Fatalf("_count request failed, %s", err)
	}
	defer resp.Body.Close()
	var r countResponse
	json.NewDecoder(resp.Body).Decode(&r)
	if r.Count != 1 {
		t.Errorf("mismatched doc count, expected 1, got %d", r.Count)
	}
}
