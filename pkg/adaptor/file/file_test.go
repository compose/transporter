package file

import (
	"fmt"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
)

func TestDescription(t *testing.T) {
	f := File{}
	if f.Description() != description {
		t.Errorf("unexpected Description, expected %s, got %s\n", description, f.Description())
	}
}

func TestSampleConfig(t *testing.T) {
	f := File{}
	if f.SampleConfig() != sampleConfig {
		t.Errorf("unexpected SampleConfig, expected %s, got %s\n", sampleConfig, f.SampleConfig())
	}
}

func TestInit(t *testing.T) {
	if _, err := adaptor.CreateAdaptor(
		"file",
		"test",
		adaptor.Config{"uri": "stdout://", "namespace": "test.test"},
		pipe.NewPipe(nil, "test"),
	); err != nil {
		t.Fatalf("unable to create File Adaptor, %s\n", err)
	}
}

var (
	startTests = []struct {
		config      adaptor.Config // input
		expectedErr error          // expected error
	}{
		{
			adaptor.Config{"uri": fmt.Sprintf("file://%s", filepath.Join("testdata", "start_test.json"))},
			nil,
		},
	}
)

func TestStart(t *testing.T) {
	for _, st := range startTests {
		sourcePipe := pipe.NewPipe(nil, "test")
		sinkPipe := pipe.NewPipe(sourcePipe, "test/sink")
		mockPipe := &MockPipe{}
		a, err := adaptor.CreateAdaptor(
			"file",
			"test",
			st.config,
			sourcePipe,
		)
		if err != nil {
			t.Fatalf("unable to create File Adaptor, %s\n", err)
		}
		go sinkPipe.Listen(mockPipe.writeMessage, regexp.MustCompile(".*"))
		if err := a.Start(); err != nil {
			t.Errorf("unexpected Start error, %s\n", err)
		}
		sinkPipe.Stop()
		if mockPipe.msgCount != 10 {
			t.Errorf("unexpected message count, expected %d, got %d\n", 10, mockPipe.msgCount)
		}
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

func TestListen(t *testing.T) {
	sourcePipe := pipe.NewPipe(nil, "test")
	a, err := adaptor.CreateAdaptor(
		"file",
		"test",
		adaptor.Config{"uri": fmt.Sprintf("file://%s", filepath.Join("testdata", "listen_test.json"))},
		sourcePipe,
	)
	if err != nil {
		t.Fatalf("unable to create File Adaptor, %s\n", err)
	}
	sinkPipe := pipe.NewPipe(sourcePipe, "test/listen")
	mockWriter := &MockWriter{}
	f := &File{
		uri:         "",
		writer:      mockWriter,
		client:      &MockClient{},
		path:        "test/listen",
		pipe:        sinkPipe,
		doneChannel: make(chan struct{}),
	}
	go f.Listen()
	if err := a.Start(); err != nil {
		t.Errorf("unexpected Start error, %s\n", err)
	}
	f.Stop()
	if mockWriter.msgCount != 2 {
		t.Errorf("unexpected message count, expected %d, got %d\n", 2, mockWriter.msgCount)
	}
}

func TestStop(t *testing.T) {
	a, err := adaptor.CreateAdaptor(
		"file",
		"test",
		adaptor.Config{"uri": fmt.Sprintf("file://%s", filepath.Join("testdata", "listen_test.json"))},
		pipe.NewPipe(nil, "test"),
	)
	if err != nil {
		t.Fatalf("unable to create File Adaptor, %s\n", err)
	}
	a.Listen()
	if err := a.Stop(); err != nil {
		t.Errorf("unable to Stop adaptor, %s\n", err)
	}
}
