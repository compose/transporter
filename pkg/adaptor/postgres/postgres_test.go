package postgres

import (
	"regexp"
	"testing"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/ops"
	"github.com/compose/transporter/pkg/pipe"
)

func TestDescription(t *testing.T) {
	p := &Postgres{}
	if p.Description() != description {
		t.Errorf("unexpected Description, expected %s, got %s\n", description, p.Description())
	}
}

func TestSampleConfig(t *testing.T) {
	p := &Postgres{}
	if p.SampleConfig() != sampleConfig {
		t.Errorf("unexpected SampleConfig, expected %s, got %s\n", sampleConfig, p.SampleConfig())
	}
}

func TestInit(t *testing.T) {
	if _, err := adaptor.CreateAdaptor(
		"postgres",
		"test",
		adaptor.Config{"uri": DefaultURI, "namespace": "test.test"},
		pipe.NewPipe(nil, "test"),
	); err != nil {
		t.Fatalf("unable to create Postgres Adaptor, %s\n", err)
	}
}

type MockPipe struct {
	msgCount int
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

func (c *MockClient) Close() {
	// no op
}

type MockSession struct {
}

type MockReader struct {
	msgCount int
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

func (r *MockReader) Read(filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan message.Msg, error) {
		out := make(chan message.Msg)
		go func() {
			defer close(out)
			for i := 0; i < r.msgCount; i++ {
				if filterFn("test") {
					out <- message.From(ops.Insert, "test", map[string]interface{}{"_id": i})
				}
			}
			if filterFn("information_schema.blah") {
				out <- message.From(ops.Insert, "information_schema.blah", map[string]interface{}{"bad": "message"})
			}
			if filterFn("pg_catalog.blah") {
				out <- message.From(ops.Insert, "pg_catalog.blah", map[string]interface{}{"bad": "message"})
			}
			return
		}()
		return out, nil
	}
}

func TestStart(t *testing.T) {
	sourcePipe := pipe.NewPipe(nil, "test")
	sinkPipe := pipe.NewPipe(sourcePipe, "test/sink")
	mockPipe := &MockPipe{}
	testCount := 10
	p := &Postgres{
		path:       "test",
		pipe:       sourcePipe,
		client:     &MockClient{},
		reader:     &MockReader{msgCount: testCount},
		tableMatch: regexp.MustCompile(".*"),
	}
	go sinkPipe.Listen(mockPipe.writeMessage, regexp.MustCompile(".*"))
	if err := p.Start(); err != nil {
		t.Errorf("unexpected Start error, %s\n", err)
	}
	sinkPipe.Stop()
	if mockPipe.msgCount != testCount {
		t.Errorf("unexpected message count, expected %d, got %d\n", testCount, mockPipe.msgCount)
	}
}

func TestListen(t *testing.T) {
	sourcePipe := pipe.NewPipe(nil, "test")
	sinkPipe := pipe.NewPipe(sourcePipe, "test/listen")
	mockWriter := &MockWriter{}
	p := &Postgres{
		path:       "test",
		pipe:       sinkPipe,
		client:     &MockClient{},
		writer:     mockWriter,
		tableMatch: regexp.MustCompile(".*"),
	}
	go p.Listen()
	sourcePipe.Send(message.From(ops.Insert, "test", map[string]interface{}{}))
	if err := p.Stop(); err != nil {
		t.Fatalf("failed to stop, %s", err)
	}
	if mockWriter.msgCount != 1 {
		t.Errorf("unexpected message count, expected %d, got %d\n", 1, mockWriter.msgCount)
	}
}

var stopTests = []struct {
	config      adaptor.Config // input
	expectedErr error          // expected error
}{
	{
		adaptor.Config{"uri": DefaultURI, "namespace": "test.test"},
		nil,
	},
	{
		adaptor.Config{"uri": DefaultURI, "namespace": "test.test", "bulk": true},
		nil,
	},
}

func TestStop(t *testing.T) {
	p := &Postgres{
		path:       "test",
		pipe:       pipe.NewPipe(nil, "test"),
		client:     &MockClient{},
		writer:     &MockWriter{},
		tableMatch: regexp.MustCompile(".*"),
	}
	go p.Listen()
	if err := p.Stop(); err != nil {
		t.Errorf("unable to Stop adaptor, %s\n", err)
	}
}
