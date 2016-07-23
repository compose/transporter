// Copyright 2013 Matthew Baird
// Licensed under the Apache License, Version 2.0 (the "License"); // you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package elastigo

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/bmizerany/assert"
)

func TestQueryString(t *testing.T) {
	// Test nil argument
	s, err := Escape(nil)
	assert.T(t, s == "" && err == nil, fmt.Sprintf("Nil should not fail and yield empty string"))

	// Test single string argument
	s, err = Escape(map[string]interface{}{"foo": "bar"})
	exp := "foo=bar"
	assert.T(t, s == exp && err == nil, fmt.Sprintf("Expected %s, got: %s", exp, s))

	// Test single int argument
	s, err = Escape(map[string]interface{}{"foo": int(1)})
	exp = "foo=1"
	assert.T(t, s == exp && err == nil, fmt.Sprintf("Expected %s, got: %s", exp, s))

	// Test single int64 argument
	s, err = Escape(map[string]interface{}{"foo": int64(1)})
	exp = "foo=1"
	assert.T(t, s == exp && err == nil, fmt.Sprintf("Expected %s, got: %s", exp, s))

	// Test single int32 argument
	s, err = Escape(map[string]interface{}{"foo": int32(1)})
	exp = "foo=1"
	assert.T(t, s == exp && err == nil, fmt.Sprintf("Expected %s, got: %s", exp, s))

	// Test single float64 argument
	s, err = Escape(map[string]interface{}{"foo": float64(3.141592)})
	exp = "foo=3.141592"
	assert.T(t, s == exp && err == nil, fmt.Sprintf("Expected %s, got: %s", exp, s))

	// Test single float32 argument
	s, err = Escape(map[string]interface{}{"foo": float32(3.141592)})
	exp = "foo=3.141592"
	assert.T(t, s == exp && err == nil, fmt.Sprintf("Expected %s, got: %s", exp, s))

	// Test single []string argument
	s, err = Escape(map[string]interface{}{"foo": []string{"bar", "baz"}})
	exp = "foo=bar%2Cbaz"
	assert.T(t, s == exp && err == nil, fmt.Sprintf("Expected %s, got: %s", exp, s))

	// Test combination of all arguments
	s, err = Escape(map[string]interface{}{
		"foo":  "bar",
		"bar":  1,
		"baz":  3.141592,
		"test": []string{"a", "b"},
	})
	// url.Values also orders arguments alphabetically.
	exp = "bar=1&baz=3.141592&foo=bar&test=a%2Cb"
	assert.T(t, s == exp && err == nil, fmt.Sprintf("Expected %s, got: %s", exp, s))

	// Test invalid datatype
	s, err = Escape(map[string]interface{}{"foo": []int{}})
	assert.T(t, err != nil, fmt.Sprintf("Expected err to not be nil"))
}

func TestDoResponseError(t *testing.T) {
	v := make(map[string]string)
	conn := NewConn()
	req, _ := conn.NewRequest("GET", "http://mock.com", "")
	req.Client = http.DefaultClient
	defer func() {
		req.Client.Transport = http.DefaultTransport
	}()

	// application/json
	req.Client.Transport = newMockTransport(500, "application/json", `{"error":"internal_server_error"}`)
	res, bodyBytes, err := req.DoResponse(&v)
	assert.NotEqual(t, nil, res)
	assert.Equal(t, nil, err)
	assert.Equal(t, 500, res.StatusCode)
	assert.Equal(t, "application/json", res.Header.Get("Content-Type"))
	assert.Equal(t, "internal_server_error", v["error"])
	assert.Equal(t, []byte(`{"error":"internal_server_error"}`), bodyBytes)

	// text/html
	v = make(map[string]string)
	req.Client.Transport = newMockTransport(500, "text/html", "HTTP 500 Internal Server Error")
	res, bodyBytes, err = req.DoResponse(&v)
	assert.T(t, res == nil, fmt.Sprintf("Expected nil, got: %v", res))
	assert.NotEqual(t, nil, err)
	assert.Equal(t, 0, len(v))
	assert.Equal(t, []byte("HTTP 500 Internal Server Error"), bodyBytes)
	assert.Equal(t, fmt.Errorf(http.StatusText(500)), err)

	//  mime error
	v = make(map[string]string)
	req.Client.Transport = newMockTransport(500, "", "HTTP 500 Internal Server Error")
	res, bodyBytes, err = req.DoResponse(&v)
	assert.T(t, res == nil, fmt.Sprintf("Expected nil, got: %v", res))
	assert.NotEqual(t, nil, err)
	assert.Equal(t, 0, len(v))
	assert.Equal(t, []byte("HTTP 500 Internal Server Error"), bodyBytes)
	assert.NotEqual(t, fmt.Errorf(http.StatusText(500)), err)
}

type mockTransport struct {
	statusCode  int
	contentType string
	body        string
}

func newMockTransport(statusCode int, contentType, body string) http.RoundTripper {
	return &mockTransport{
		statusCode:  statusCode,
		contentType: contentType,
		body:        body,
	}
}

func (t *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	response := &http.Response{
		Header:     make(http.Header),
		Request:    req,
		StatusCode: t.statusCode,
	}
	response.Header.Set("Content-Type", t.contentType)
	response.Body = ioutil.NopCloser(strings.NewReader(t.body))
	return response, nil
}

func TestSetBodyGzip(t *testing.T) {
	s := "foo"

	// test []byte
	expB := []byte(s)
	actB, err := gzipHelper(t, expB)
	assert.T(t, err == nil, fmt.Sprintf("Expected err to be nil"))
	assert.T(t, bytes.Compare(actB, expB) == 0, fmt.Sprintf("Expected: %s, got: %s", expB, actB))

	// test string
	expS := s
	actS, err := gzipHelper(t, expS)
	assert.T(t, err == nil, fmt.Sprintf("Expected err to be nil"))
	assert.T(t, string(actS) == expS, fmt.Sprintf("Expected: %s, got: %s", expS, actS))

	// test io.Reader
	expR := strings.NewReader(s)
	actR, err := gzipHelper(t, expR)
	assert.T(t, err == nil, fmt.Sprintf("Expected err to be nil"))
	assert.T(t, bytes.Compare([]byte(s), actR) == 0, fmt.Sprintf("Expected: %s, got: %s", s, actR))

	// test other
	expO := testStruct{Name: "Travis"}
	actO, err := gzipHelper(t, expO)
	assert.T(t, err == nil, fmt.Sprintf("Expected err to not be nil"))
	assert.T(t, bytes.Compare([]byte(`{"name":"Travis"}`), actO) == 0, fmt.Sprintf("Expected: %s, got: %s", s, actO))
}

type testStruct struct {
	Name string `json:"name"`
}

func gzipHelper(t *testing.T, data interface{}) ([]byte, error) {
	r, err := http.NewRequest("GET", "http://google.com", nil)
	if err != nil {
		return nil, err
	}

	// test string
	req := &Request{
		Request: r,
	}

	err = req.SetBodyGzip(data)
	if err != nil {
		return nil, err
	}

	gr, err := gzip.NewReader(req.Body)
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(gr)
}
