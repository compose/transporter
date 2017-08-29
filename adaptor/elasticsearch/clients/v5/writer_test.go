package v5

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/adaptor/elasticsearch/clients"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

const (
	defaultURL         = "http://127.0.0.1:9200"
	defaultIndex       = "test_v5"
	testType           = "test"
	parentDefaultIndex = "parent_test_v5"
)

var (
	testURL = os.Getenv("ES_V5_URL")
)

func fullURL(suffix string) string {
	return fmt.Sprintf("%s/%s%s", testURL, defaultIndex, suffix)
}

func parentFullURL(suffix string) string {
	return fmt.Sprintf("%s/%s%s", testURL, parentDefaultIndex, suffix)
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
func createMapping() error {
	// create a simple mapping one company has many employees
	var mapping = []byte(`{"mappings": {"company": {}, "employee": {"_parent": {"type": "company"} } } }`)
	req, _ := http.NewRequest("PUT", parentFullURL(""), bytes.NewBuffer(mapping))
	_, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Debugf("creating Elasticsearch Mapping request failed, %s", err)
	}
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
	confirms, cleanup := adaptor.MockConfirmWrites()
	defer adaptor.VerifyWriteConfirmed(cleanup, t)
	opts := &clients.ClientOptions{
		URLs:       []string{testURL},
		HTTPClient: http.DefaultClient,
		Index:      defaultIndex,
	}
	vc := clients.Clients["v5"]
	w, _ := vc.Creator(opts)
	w.Write(
		message.WithConfirms(
			confirms,
			message.From(ops.Insert, testType, map[string]interface{}{"hello": "world"})),
	)(nil)
	w.Write(
		message.WithConfirms(
			confirms,
			message.From(ops.Insert, testType, map[string]interface{}{"_id": "booya", "hello": "world"})),
	)(nil)
	w.Write(
		message.WithConfirms(
			confirms,
			message.From(ops.Update, testType, map[string]interface{}{"_id": "booya", "hello": "goodbye"})),
	)(nil)
	w.Write(
		message.WithConfirms(
			confirms,
			message.From(ops.Delete, testType, map[string]interface{}{"_id": "booya", "hello": "goodbye"})),
	)(nil)
	w.(client.Closer).Close()

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

func TestWithParentWriter(t *testing.T) {
	confirms, cleanup := adaptor.MockConfirmWrites()
	defer adaptor.VerifyWriteConfirmed(cleanup, t)
	opts := &clients.ClientOptions{
		URLs:       []string{testURL},
		HTTPClient: http.DefaultClient,
		Index:      parentDefaultIndex,
		ParentID:   "parent_id",
	}
	createMapping()
	vc := clients.Clients["v5"]
	w, _ := vc.Creator(opts)
	w.Write(
		message.WithConfirms(
			confirms,
			message.From(ops.Insert, "company", map[string]interface{}{"_id": "9g2g", "name": "gingerbreadhouse"})),
	)(nil)
	w.Write(
		message.WithConfirms(
			confirms,
			message.From(ops.Insert, "employee", map[string]interface{}{"_id": "9g6g", "name": "witch", "parent_id": "gingerbreadhouse"})),
	)(nil)
	w.(client.Closer).Close()
	if _, err := http.Get(parentFullURL("/_refresh")); err != nil {
		t.Fatalf("_refresh request failed, %s", err)
	}
	time.Sleep(1 * time.Second)
	countResp, err := http.Get(parentFullURL("/_count"))
	if err != nil {
		t.Fatalf("_count request failed, %s", err)
	}
	defer countResp.Body.Close()
	var r countResponse
	json.NewDecoder(countResp.Body).Decode(&r)
	if r.Count != 2 {
		t.Errorf("mismatched doc count, expected 2, got %d", r.Count)
	}
	employeeResp, err := http.Get(parentFullURL("/employee/_search"))
	if err != nil {
		t.Fatalf("_count request failed, %s", err)
	}
	defer employeeResp.Body.Close()
	type Employee struct {
		Hits struct {
			Hits []struct {
				ID     string `json:"_id"`
				Parent string `json:"_parent"`
				Type   string `json:"_type"`
			} `json:"hits"`
		} `json:"hits"`
	}
	var par Employee
	json.NewDecoder(employeeResp.Body).Decode(&par)
	if par.Hits.Hits[0].Parent != "gingerbreadhouse" {
		t.Errorf("mismatched _parent, got %d", par.Hits.Hits[0].Parent)
	}
}
