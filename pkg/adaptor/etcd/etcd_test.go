package etcd

import (
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/ops"
	"github.com/compose/transporter/pkg/pipe"
)

func TestDescription(t *testing.T) {
	e := Etcd{}
	// confirm Etcd fulfills the Describable interface
	e.Description()
}

func TestSampleConfig(t *testing.T) {
	e := Etcd{}
	// confirm Etcd fulfills the Describable interface
	e.SampleConfig()
}

func TestInit(t *testing.T) {
	if _, err := adaptor.CreateAdaptor(
		"etcd",
		"test",
		adaptor.Config{"uri": "http://127.0.0.1:2379", "namespace": "test.test"},
		pipe.NewPipe(nil, "test"),
	); err != nil {
		t.Fatalf("unable to create etcd Adaptor, %s\n", err)
	}
}

func TestConnect(t *testing.T) {
	e := Etcd{client: &MockClient{}}
	// confirm Etcd fulfills the Connectable interface
	e.Connect()
}

func TestStart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping etcd Start in short mode")
	}

	sourcePipe := pipe.NewPipe(nil, "test")
	sinkPipe := pipe.NewPipe(sourcePipe, "test/sink")
	mockPipe := &MockPipe{}
	a, err := adaptor.CreateAdaptor(
		"etcd",
		"test",
		adaptor.Config{"uri": "http://localhost:2379", "namespace": "subkey.test"},
		sourcePipe,
	)
	if err != nil {
		t.Fatalf("unable to create etcd Adaptor, %s\n", err)
	}
	go sinkPipe.Listen(mockPipe.writeMessage, regexp.MustCompile(".*"))
	if err := a.Start(); err != nil {
		t.Errorf("unexpected Start error, %s\n", err)
	}
	sinkPipe.Stop()
	if mockPipe.msgCount != len(expectedSubKeyMsgs) {
		t.Errorf("unexpected message count, expected %d, got %d\n", len(expectedSubKeyMsgs), mockPipe.msgCount)
	}
}

func TestListen(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping etcd Listen in short mode")
	}

	sourcePipe := pipe.NewPipe(nil, "test")
	a, err := adaptor.CreateAdaptor(
		"etcd",
		"test",
		adaptor.Config{"uri": "http://localhost:2379", "namespace": "subkey.test"},
		sourcePipe,
	)
	if err != nil {
		t.Fatalf("unable to create etcd Adaptor, %s\n", err)
	}
	sinkPipe := pipe.NewPipe(sourcePipe, "test/listen")
	mockWriter := &MockWriter{}
	e := &Etcd{
		rootKey:     "subkey",
		subKeyMatch: regexp.MustCompile(".*"),
		writer:      mockWriter,
		client:      &MockClient{},
		path:        "test/listen",
		pipe:        sinkPipe,
		doneChannel: make(chan struct{}),
	}
	go e.Listen()
	if err := a.Start(); err != nil {
		t.Errorf("unexpected Start error, %s\n", err)
	}
	e.Stop()
	if mockWriter.msgCount != len(expectedSubKeyMsgs) {
		t.Errorf("unexpected message count, expected %d, got %d\n", len(expectedSubKeyMsgs), mockWriter.msgCount)
	}
}

func TestAdaptorError(t *testing.T) {
	mockErrWriter := &MockErrWriter{}
	sourcePipe := pipe.NewPipe(nil, "test")
	sinkPipe := pipe.NewPipe(sourcePipe, "test/listen")
	e := &Etcd{
		rootKey:     "subkey",
		subKeyMatch: regexp.MustCompile(".*"),
		writer:      mockErrWriter,
		client:      &MockClient{},
		path:        "test/listen",
		pipe:        sinkPipe,
		doneChannel: make(chan struct{}),
	}

	eCounter := &ErrorCounter{}
	done := make(chan struct{})
	go eCounter.startErrorListener(sinkPipe.Err, done)
	go e.Listen()
	sourcePipe.Send(message.From(ops.Insert, "test.listen", nil))
	time.Sleep(1 * time.Second)
	close(done)
	e.Stop()

	if eCounter.errorCount != 1 {
		t.Errorf("bad errorCount, expected 1, got %d", eCounter.errorCount)
	}
}

type MockPipe struct {
	msgCount int
	Err      chan error
}

func (m *MockPipe) writeMessage(msg message.Msg) (message.Msg, error) {
	m.msgCount++
	return msg, nil
}

type MockClient struct {
}

func (c *MockClient) Connect() (client.Session, error) {
	return &MockSession{}, nil
}

type MockSession struct {
}

func (s *MockSession) Close() {}

type MockWriter struct {
	msgCount int
}

func (w *MockWriter) Write(msg message.Msg) func(client.Session) error {
	return func(s client.Session) error {
		w.msgCount++
		return nil
	}
}

type MockErrWriter struct{}

func (w *MockErrWriter) Write(msg message.Msg) func(client.Session) error {
	return func(s client.Session) error {
		return errors.New("you shall not pass")
	}
}

type ErrorCounter struct {
	errorCount int
}

func (e *ErrorCounter) startErrorListener(cherr chan error, done chan struct{}) {
	for {
		select {
		case err := <-cherr:
			if _, ok := err.(adaptor.Error); ok {
				e.errorCount++
			}
		case <-done:
			return
		}
	}
}
