package elasticsearch

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"testing"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/ops"
	"github.com/compose/transporter/pkg/pipe"
)

const (
	DefaultURL = "http://127.0.0.1:9200"
)

var (
	mockElasticsearch = &Elasticsearch{}
	TestURL           = os.Getenv("ES_V5_URL")
)

var errorTests = []struct {
	name     string
	expected string
	e        error
}{
	{
		"InvalidURIError",
		"Invalid URI, blah",
		InvalidURIError{"blah"},
	},
	{
		"ConnectionError",
		"failed to connect to http://goingnowhere:9200",
		ConnectionError{"http://goingnowhere:9200"},
	},
	{
		"VersionError",
		"unable to determine version from http://localhost:9200, its bad",
		VersionError{"http://localhost:9200", "", "its bad"},
	},
	{
		"VersionError with version",
		"http://localhost:9200 running 0.9.2, its bad",
		VersionError{"http://localhost:9200", "0.9.2", "its bad"},
	},
}

func TestErrors(t *testing.T) {
	for _, et := range errorTests {
		if et.e.Error() != et.expected {
			t.Errorf("[%s] wrong Error(), expected %s, got %s", et.name, et.expected, et.e.Error())
		}
	}
}

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

var initTests = []struct {
	name string
	cfg  adaptor.Config
	err  error
}{
	{
		"base config",
		adaptor.Config{"uri": goodVersionServer.URL, "namespace": "test.test"},
		nil,
	},
	{
		"timeout config",
		adaptor.Config{"uri": goodVersionServer.URL, "namespace": "test.test", "timeout": "60s"},
		nil,
	},
	{
		"bad namespace",
		adaptor.Config{"uri": goodVersionServer.URL, "namespace": "badNs"},
		adaptor.NewError(adaptor.CRITICAL, "test", "can't split namespace into index and typeMatch (malformed namespace, expected a '.' deliminated string)", nil),
	},
}

func TestInit(t *testing.T) {
	defer goodVersionServer.Close()
	for _, it := range initTests {
		if _, err := adaptor.CreateAdaptor(
			"elasticsearch",
			"test",
			it.cfg,
			pipe.NewPipe(nil, "test"),
		); err != it.err {
			t.Errorf("[%s] bad error, expected %q, got %q", it.name, it.err, err)
		}
	}
}

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

var badClientTests = []struct {
	name    string
	cfg     adaptor.Config
	err     error
	cleanup func()
}{
	{
		"bad URI",
		adaptor.Config{"uri": "%gh&%ij", "namespace": "test.test"},
		InvalidURIError{"%gh&%ij"},
		func() {},
	},
	{
		"no connection",
		adaptor.Config{"uri": "http://localhost:10000", "namespace": "test.test"},
		ConnectionError{"http://localhost:10000"},
		func() {},
	},
	{
		"empty body",
		adaptor.Config{"uri": emptyBodyServer.URL, "namespace": "test.test"},
		VersionError{emptyBodyServer.URL, "", "missing version: {}"},
		func() { emptyBodyServer.Close() },
	},
	{
		"malformed JSON",
		adaptor.Config{"uri": badJSONServer.URL, "namespace": "test.test"},
		VersionError{badJSONServer.URL, "", "malformed JSON: Hello, client"},
		func() { badJSONServer.Close() },
	},
	{
		"bad version",
		adaptor.Config{"uri": badVersionServer.URL, "namespace": "test.test"},
		VersionError{badVersionServer.URL, "not a version", "Malformed version: not a version"},
		func() { badVersionServer.Close() },
	},
	{
		"unsupported version",
		adaptor.Config{"uri": unsupportedVersionServer.URL, "namespace": "test.test"},
		VersionError{unsupportedVersionServer.URL, "0.9.2", "unsupported client"},
		func() { unsupportedVersionServer.Close() },
	},
}

func TestFailedClient(t *testing.T) {
	for _, ct := range badClientTests {
		if _, err := adaptor.CreateAdaptor(
			"elasticsearch",
			"test",
			ct.cfg,
			pipe.NewPipe(nil, "test"),
		); err == nil {
			t.Fatal("no error received but expected one")
		} else if err != (ct.err) {
			t.Errorf("[%s] wrong error\nexpected: %q\ngot: %q", ct.name, ct.err, err)
		}
		ct.cleanup()
	}
}

func TestStart(t *testing.T) {
	if err := mockElasticsearch.Start(); err == nil {
		t.Fatal("no error returned from Start but expected one")
	} else if err.Error() != "Start is unsupported for elasticsearch" {
		t.Errorf("unknown error message, got %s", err.Error())
	}
}

func TestListen(t *testing.T) {
	sourcePipe := pipe.NewPipe(nil, "test")
	sinkPipe := pipe.NewPipe(sourcePipe, "test/listen")
	mockWriter := &MockWriter{}

	e := &Elasticsearch{
		index:       "listen_db",
		typeMatch:   regexp.MustCompile(".*"),
		client:      mockWriter,
		path:        "test/listen",
		pipe:        sinkPipe,
		doneChannel: make(chan struct{}),
	}
	go e.Listen()

	sourcePipe.Send(message.From(ops.Insert, "source.test", map[string]interface{}{"hello": "world"}))

	e.Stop()
	if mockWriter.msgCount != 1 {
		t.Errorf("unexpected message count, expected %d, got %d\n", 1, mockWriter.msgCount)
	}
}

type MockWriter struct {
	msgCount int
}

func (w *MockWriter) Write(msg message.Msg) func(client.Session) error {
	return func(s client.Session) error {
		w.msgCount++
		return nil
	}
}
