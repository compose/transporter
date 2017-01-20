package v5

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/adaptor/elasticsearch/clients"
	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/ops"
)

const (
	DefaultURL = "http://127.0.0.1:9200"
	ESV5Index  = "test_v5"
)

var (
	ESV5URL = os.Getenv("ES_V5_URL")
)

func testURL(suffix string) string {
	return fmt.Sprintf("%s/%s%s", ESV5URL, ESV5Index, suffix)
}

func setup() error {
	log.Debugln("setting up tests")
	return clearTestData()
}

func clearTestData() error {
	req, _ := http.NewRequest(http.MethodDelete, testURL(""), nil)
	resp, err := http.DefaultClient.Do(req)
	log.Debugf("clearTestData response, %+v", resp)
	return err
}

func TestMain(m *testing.M) {
	if ESV5URL == "" {
		ESV5URL = DefaultURL
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

type CountResponse struct {
	Count int `json:"count"`
}

func TestWriter(t *testing.T) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	opts := &clients.ClientOptions{
		URLs:       []string{ESV5URL},
		HTTPClient: http.DefaultClient,
		Path:       "test_v5",
	}
	vc := clients.Clients["v5"]
	w, _ := vc.Creator(done, &wg, opts)
	w.Write(message.From(ops.Insert, "test_v5.test", map[string]interface{}{"hello": "world"}))(nil)
	w.Write(message.From(ops.Insert, "test_v5.test", map[string]interface{}{"_id": "booya", "hello": "world"}))(nil)
	w.Write(message.From(ops.Update, "test_v5.test", map[string]interface{}{"_id": "booya", "hello": "goodbye"}))(nil)
	w.Write(message.From(ops.Delete, "test_v5.test", map[string]interface{}{"_id": "booya", "hello": "goodbye"}))(nil)
	close(done)
	wg.Wait()

	if _, err := http.Get(testURL("/_refresh")); err != nil {
		t.Fatalf("_refresh request failed, %s", err)
	}
	time.Sleep(1 * time.Second)

	resp, err := http.Get(testURL("/_count"))
	if err != nil {
		t.Fatalf("_count request failed, %s", err)
	}
	defer resp.Body.Close()
	var r CountResponse
	json.NewDecoder(resp.Body).Decode(&r)
	if r.Count != 1 {
		t.Errorf("mismatched doc count, expected 1, got %d", r.Count)
	}
}