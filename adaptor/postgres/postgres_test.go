package postgres

import (
	"regexp"
	"testing"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
	"github.com/compose/transporter/pipe"
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

type MockReader struct {
	msgCount int
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
	mockPipe := &pipe.Mock{}
	testCount := 10
	p := &Postgres{
		path:       "test",
		pipe:       sourcePipe,
		client:     &client.Mock{},
		reader:     &MockReader{msgCount: testCount},
		tableMatch: regexp.MustCompile(".*"),
	}
	go sinkPipe.Listen(mockPipe.WriteMessage, regexp.MustCompile(".*"))
	if err := p.Start(); err != nil {
		t.Errorf("unexpected Start error, %s\n", err)
	}
	sinkPipe.Stop()
	if mockPipe.MsgCount != testCount {
		t.Errorf("unexpected message count, expected %d, got %d\n", testCount, mockPipe.MsgCount)
	}
}

func TestListen(t *testing.T) {
	sourcePipe := pipe.NewPipe(nil, "test")
	sinkPipe := pipe.NewPipe(sourcePipe, "test/listen")
	mockWriter := &client.MockWriter{}
	p := &Postgres{
		path:       "test",
		pipe:       sinkPipe,
		client:     &client.Mock{},
		writer:     mockWriter,
		tableMatch: regexp.MustCompile(".*"),
	}
	go p.Listen()
	sourcePipe.Send(message.From(ops.Insert, "test", map[string]interface{}{}))
	if err := p.Stop(); err != nil {
		t.Fatalf("failed to stop, %s", err)
	}
	if mockWriter.MsgCount != 1 {
		t.Errorf("unexpected message count, expected %d, got %d\n", 1, mockWriter.MsgCount)
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
		client:     &client.Mock{},
		writer:     &client.MockWriter{},
		tableMatch: regexp.MustCompile(".*"),
	}
	go p.Listen()
	if err := p.Stop(); err != nil {
		t.Errorf("unable to Stop adaptor, %s\n", err)
	}
}
