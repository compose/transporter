package events

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/log"
)

type counter struct {
	emitCount int64
}

func (c *counter) countEmitter() EmitFunc {
	return EmitFunc(func(event Event) error {
		atomic.AddInt64(&c.emitCount, 1)
		return nil
	})
}

func TestNewEmitter(t *testing.T) {
	c := &counter{emitCount: 0}
	events := make(chan Event, 10)
	rand.Seed(time.Now().Unix())
	expectedCount := rand.Intn(100)
	e := NewEmitter(events, c.countEmitter())
	e.Start()
	for i := 0; i < expectedCount; i++ {
		events <- NewMetricsEvent(12345, "test", i)
	}
	e.Stop()
	finalCount := atomic.LoadInt64(&c.emitCount)
	if int(finalCount) != expectedCount {
		t.Errorf("wrong number of events emitted, expected %d, got %d", expectedCount, finalCount)
	}
}

const (
	pid  = "12345"
	key  = "asdfghjk"
	path = "/metrics"
)

var eventServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	if u, p, ok := r.BasicAuth(); !ok {
		w.WriteHeader(http.StatusForbidden)
		return
	} else if u != pid || p != key {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if r.URL.Path != path {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}))

var httptests = []struct {
	name string
	emit EmitFunc
	err  error
}{
	{
		"success",
		HTTPPostEmitter(fmt.Sprintf("%s%s", eventServer.URL, path), key, pid),
		nil,
	},
	{
		"unauthorized, wrong password",
		HTTPPostEmitter(fmt.Sprintf("%s%s", eventServer.URL, path), "", pid),
		BadStatusError{http.StatusForbidden},
	},
	{
		"unauthorized, wrong user",
		HTTPPostEmitter(fmt.Sprintf("%s%s", eventServer.URL, path), key, ""),
		BadStatusError{http.StatusForbidden},
	},
	{
		"URL not found",
		HTTPPostEmitter(fmt.Sprintf("%s/badpath", eventServer.URL), key, pid),
		BadStatusError{http.StatusNotFound},
	},
	{
		"server failed",
		HTTPPostEmitter("http://127.0.0.1:8000/metrics", key, pid),
		&url.Error{
			Op:  "Post",
			URL: "http://127.0.0.1:8000/metrics",
			Err: &net.OpError{
				Op:     "dial",
				Net:    "tcp",
				Source: &MockAddr{},
				Err:    errors.New("getsockopt: connection refused"),
			},
		},
	},
}

type MockAddr struct {
}

func (a *MockAddr) Network() string {
	return "tcp"
}

func (a *MockAddr) String() string {
	return "127.0.0.1:8000"
}

func TestHTTPSever(t *testing.T) {
	defer eventServer.Close()
	for _, ht := range httptests {
		err := ht.emit(NewMetricsEvent(12345, "test", 0))
		if err == nil && err != ht.err {
			t.Errorf("[%s] mismatched error, expected:\n%+v\ngot\n%+v", ht.name, ht.err, err)
		} else if err != nil && err.Error() != ht.err.Error() {
			t.Errorf("[%s] mismatched error, expected:\n%+v\ngot:\n%+v", ht.name, ht.err, err)
		}
	}
}

func TestNoopEmitter(t *testing.T) {
	noopFunc := NoopEmitter()
	err := noopFunc(nil)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
}

const (
	mockLogCount = 10
)

type MockEvent struct {
	l     *LogRecorder
	event string
}

func (e *MockEvent) Emit() ([]byte, error) {
	return []byte(fmt.Sprintf("{\"event\":\"%s\"}", e.event)), nil
}

func (e *MockEvent) String() string {
	return e.event
}

func (e *MockEvent) Logger() log.Logger {
	return e.l
}

type LogRecorder struct {
	logCount int
	logs     []string
}

func (l *LogRecorder) Debugln(msg ...interface{}) {
	l.logCount++
	l.logs = append(l.logs, fmt.Sprint(msg...))
}
func (l *LogRecorder) Debugf(format string, msg ...interface{}) {
	l.logCount++
	l.logs = append(l.logs, fmt.Sprint(msg...))
}
func (l *LogRecorder) Infoln(msg ...interface{}) {
	l.logCount++
	l.logs = append(l.logs, fmt.Sprint(msg...))
}
func (l *LogRecorder) Infof(format string, msg ...interface{}) {
	l.logCount++
	l.logs = append(l.logs, fmt.Sprint(msg...))
}
func (l *LogRecorder) Errorln(msg ...interface{}) {
	l.logCount++
	l.logs = append(l.logs, fmt.Sprint(msg...))
}
func (l *LogRecorder) Errorf(format string, msg ...interface{}) {
	l.logCount++
	l.logs = append(l.logs, fmt.Sprint(msg...))
}
func (l *LogRecorder) Printf(format string, msg ...interface{}) {
	l.logCount++
	l.logs = append(l.logs, fmt.Sprint(msg...))
}
func (l *LogRecorder) With(key string, value interface{}) log.Logger { return l }
func (l LogRecorder) Output(calldepth int, s string) error {
	l.logCount++
	return nil
}

var emitterTests = []struct {
	name      string
	emitFunc  EmitFunc
	logFormat func(int) string
}{
	{"log", LogEmitter(), func(i int) string { return strconv.Itoa(i) }},
	{"json", JSONLogEmitter(), func(i int) string { return fmt.Sprintf("{\"event\":\"%d\"}", i) }},
}

func TestEmitters(t *testing.T) {
	for _, et := range emitterTests {
		l := &LogRecorder{}
		expectedLogs := make([]string, mockLogCount)
		for i := 0; i < mockLogCount; i++ {
			expectedLogs[i] = et.logFormat(i)
			err := et.emitFunc(&MockEvent{l, strconv.Itoa(i)})
			if err != nil {
				t.Errorf("[%s] unexpected error: %s", et.name, err)
			}
		}

		if l.logCount != mockLogCount {
			t.Errorf("[%s] wrong log count: expected %d, got %d", et.name, mockLogCount, l.logCount)
		}

		if fmt.Sprintf("%v", l.logs) != fmt.Sprintf("%v", expectedLogs) {
			t.Errorf("[%s] wrong logs sent: expected %+v, got %+v", et.name, expectedLogs, l.logs)
		}

	}
}
