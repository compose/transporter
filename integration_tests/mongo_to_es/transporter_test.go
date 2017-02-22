// +build integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
)

func TestMongoToElasticsearchDocCount(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/enron/emails/_count", os.Getenv("ES_ENRON_SINK_URI")), nil)
	req.SetBasicAuth(os.Getenv("ES_ENRON_SINK_USER"), os.Getenv("ES_ENRON_SINK_PASSWORD"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error, %s", err)
	}
	defer resp.Body.Close()
	var result struct {
		Count int `json:"count"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	if result.Count != 501514 {
		t.Errorf("bad email count, expected 501514, got %d", result.Count)
	}
}
