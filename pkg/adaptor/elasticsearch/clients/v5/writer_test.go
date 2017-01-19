package v5

import (
	"encoding/json"
	"net/http"
	"os"
	"sync"
	"testing"

	"github.com/compose/transporter/pkg/adaptor/elasticsearch/clients"
	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/ops"
)

func setup() error {
	log.Infoln("setting up tests")
	return clearTestData()
}

func clearTestData() error {
	req, _ := http.NewRequest(http.MethodDelete, "http://127.0.0.1:9200/test_v5", nil)
	resp, err := http.DefaultClient.Do(req)
	log.Debugf("clearTestData response, %+v", resp)
	return err
}

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		log.Errorf("unable to setup tests, %s", err)
		os.Exit(1)
	}
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func shutdown() {
	log.Infoln("shutting down tests")
	clearTestData()
	log.Infoln("tests shutdown complete")
}

type CountResponse struct {
	Count int `json:"count"`
}

func TestWriter(t *testing.T) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	opts := &clients.ClientOptions{
		URLs:       []string{"http://127.0.0.1:9200"},
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

	if _, err := http.Get("http://127.0.0.1:9200/test_v5/_refresh"); err != nil {
		t.Fatalf("_refresh request failed, %s", err)
	}

	resp, err := http.Get("http://127.0.0.1:9200/test_v5/_count")
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
