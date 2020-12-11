package elasticsearch

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
)

var (
	mockElasticsearch = &Elasticsearch{}
)

func TestDescription(t *testing.T) {
	if mockElasticsearch.Description() != description {
		t.Errorf("wrong description returned, expected %s, got %s", description, mockElasticsearch.Description())
	}
}

func TestSampleConfig(t *testing.T) {
	if mockElasticsearch.SampleConfig() != sampleConfig {
		t.Errorf("wrong config returned, expected %s, got %s", sampleConfig, mockElasticsearch.SampleConfig())
	}
}

var goodVersionServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "{\"version\":{\"number\":\"5.0.0\"}}")
}))

var (
	testUser = "user"
	testPwd  = "pwd"
	authURI  = func() string {
		uri, _ := url.Parse(authedServer.URL)
		uri.User = url.UserPassword(testUser, testPwd)
		return uri.String()
	}
)
var authedServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	u, p, ok := r.BasicAuth()
	if !ok || u != testUser || p != testPwd {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	fmt.Fprint(w, "{\"version\":{\"number\":\"5.0.0\"}}")
}))

var emptyBodyServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "{}")
}))

var badJSONServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Hello, client")
}))

var badVersionServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "{\"version\":{\"number\":\"not a version\"}}")
}))

var unsupportedVersionServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "{\"version\":{\"number\":\"0.9.2\"}}")
}))

var clientTests = []struct {
	name string
	cfg  adaptor.Config
	err  error
}{
	{
		"base config",
		adaptor.Config{"uri": goodVersionServer.URL},
		nil,
	},
	{
		"timeout config",
		adaptor.Config{"uri": goodVersionServer.URL, "timeout": "60s"},
		nil,
	},
	{
		"authed URI",
		adaptor.Config{"uri": authURI()},
		nil,
	},
	{
		"parent_id config",
		adaptor.Config{"uri": authURI(), "parent_id": "parent_id"},
		nil,
	},
	{
		"bad URI",
		adaptor.Config{"uri": "%gh&%ij"},
		client.InvalidURIError{URI: "%gh&%ij", Err: `parse %gh&%ij: invalid URL escape "%gh"`},
	},
	{
		"no connection",
		adaptor.Config{"uri": "http://localhost:7200"},
		client.ConnectError{Reason: "http://localhost:7200"},
	},
	{
		"empty body",
		adaptor.Config{"uri": fmt.Sprintf("%s/test", emptyBodyServer.URL)},
		client.VersionError{URI: emptyBodyServer.URL, V: "", Err: "missing version: {}"},
	},
	{
		"malformed JSON",
		adaptor.Config{"uri": fmt.Sprintf("%s/test", badJSONServer.URL)},
		client.VersionError{URI: badJSONServer.URL, V: "", Err: "malformed JSON: Hello, client"},
	},
	{
		"bad version",
		adaptor.Config{"uri": badVersionServer.URL},
		client.VersionError{URI: badVersionServer.URL, V: "not a version", Err: "Malformed version: not a version"},
	},
	{
		"unsupported version",
		adaptor.Config{"uri": unsupportedVersionServer.URL},
		client.VersionError{URI: unsupportedVersionServer.URL, V: "0.9.2", Err: "unsupported client"},
	},
}

func TestInit(t *testing.T) {
	defer func() {
		goodVersionServer.Close()
		authedServer.Close()
		emptyBodyServer.Close()
		badJSONServer.Close()
		badVersionServer.Close()
		unsupportedVersionServer.Close()
	}()
	for _, ct := range clientTests {
		c, err := adaptor.GetAdaptor("elasticsearch", ct.cfg)
		if err != nil {
			t.Errorf("[%s] unexpected error: %q", ct.name, err)
		}
		if _, err := c.Client(); err != nil {
			t.Errorf("unexpected Client() error, %s", err)
		}
		rerr := adaptor.ErrFuncNotSupported{Name: "Reader()", Func: "elasticsearch"}
		if _, err := c.Reader(); err != rerr {
			t.Errorf("wrong Reader() error, expected %s, got %s", rerr, err)
		}
		if _, err := c.Writer(nil, nil); err != ct.err {
			t.Errorf("[%s] wrong error\nexpected: %q\ngot: %q", ct.name, ct.err, err)
		}
	}
}

