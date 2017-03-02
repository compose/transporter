package elasticsearch

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const (
	awsHmacHeader = "AWS4-HMAC-SHA256 Credential=accessKeyID"
	awsAccessKey  = "accessKeyID"
	awsSecretKey  = "secretAccessKey"
)

var mockServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	expectAWSRequest := r.URL.Path == "/aws"
	if isAWSRequest(r) != expectAWSRequest {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	fmt.Fprint(w, "{\"ok\":1}")
}))

func isAWSRequest(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get("Authorization"), awsHmacHeader) &&
		r.Header.Get("X-Amz-Content-Sha256") != "" &&
		r.Header.Get("X-Amz-Date") != ""
}

var transportTests = []struct {
	path string
	c    *http.Client
}{
	{
		"/aws",
		&http.Client{Transport: newTransport(awsAccessKey, awsSecretKey)},
	},
	{
		"/other",
		&http.Client{Transport: newTransport("", "")},
	},
}

func TestTransport(t *testing.T) {
	defer mockServer.Close()

	for _, tt := range transportTests {
		req, err := http.NewRequest(
			http.MethodGet,
			fmt.Sprintf("%s%s", mockServer.URL, tt.path),
			nil,
		)
		if err != nil {
			t.Fatalf("unable to build request, %s", err)
		}
		resp, err := tt.c.Do(req)
		if err != nil {
			t.Errorf("failed to send request, %s", err)
		} else if resp.StatusCode == http.StatusBadRequest {
			t.Errorf("bad request sent for %s", tt.path)
		}
	}
}
