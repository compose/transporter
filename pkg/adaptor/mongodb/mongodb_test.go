package mongodb

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
)

var (
	startTestData = &TestData{"start_test", "test", 10}
	startTests    = []struct {
		config      adaptor.Config // input
		expectedErr error          // expected error
	}{
		{
			adaptor.Config{"uri": "mongodb://localhost:27017", "namespace": fmt.Sprintf("%s.test", startTestData.DB)},
			nil,
		},
	}

	listenTestData = &TestData{"listen_test", "test", 10}
)

func TestDescription(t *testing.T) {
	m := MongoDB{}
	if m.Description() != description {
		t.Errorf("unexpected Description, expected %s, got %s\n", description, m.Description())
	}
}

func TestSampleConfig(t *testing.T) {
	m := MongoDB{}
	if m.SampleConfig() != sampleConfig {
		t.Errorf("unexpected SampleConfig, expected %s, got %s\n", sampleConfig, m.SampleConfig())
	}
}

func TestInit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping MongoDB Init in short mode")
	}

	if _, err := adaptor.CreateAdaptor(
		"mongodb",
		"test",
		adaptor.Config{"uri": "mongodb://localhost:27017", "namespace": "test.test"},
		pipe.NewPipe(nil, "test"),
	); err != nil {
		t.Fatalf("unable to create MongoDB Adaptor, %s\n", err)
	}
}

func TestStart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping MongoDB Start in short mode")
	}

	for _, st := range startTests {
		sourcePipe := pipe.NewPipe(nil, startTestData.DB)
		sinkPipe := pipe.NewPipe(sourcePipe, "test/sink")
		mockPipe := &MockPipe{}
		a, err := adaptor.CreateAdaptor(
			"mongodb",
			startTestData.DB,
			st.config,
			sourcePipe,
		)
		if err != nil {
			t.Fatalf("unable to create MongoDB Adaptor, %s\n", err)
		}
		go sinkPipe.Listen(mockPipe.writeMessage, regexp.MustCompile(".*"))
		if err := a.Start(); err != nil {
			t.Errorf("unexpected Start error, %s\n", err)
		}
		sinkPipe.Stop()
		if mockPipe.msgCount != startTestData.InsertCount {
			t.Errorf("unexpected message count, expected %d, got %d\n", startTestData.InsertCount, mockPipe.msgCount)
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
	if testing.Short() {
		t.Skip("skipping MongoDB Listen in short mode")
	}

	sourcePipe := pipe.NewPipe(nil, startTestData.DB)
	a, err := adaptor.CreateAdaptor(
		"mongodb",
		startTestData.DB,
		adaptor.Config{"uri": "mongodb://localhost:27017", "namespace": fmt.Sprintf("%s.%s", listenTestData.DB, listenTestData.C)},
		sourcePipe,
	)
	if err != nil {
		t.Fatalf("unable to create MongoDB Adaptor, %s\n", err)
	}
	sinkPipe := pipe.NewPipe(sourcePipe, "test/listen")
	mockWriter := &MockWriter{}
	m := &MongoDB{
		database:        listenTestData.DB,
		collectionMatch: regexp.MustCompile(".*"),
		writer:          mockWriter,
		client:          &MockClient{},
		path:            "test/listen",
		pipe:            sinkPipe,
		doneChannel:     make(chan struct{}),
	}
	go m.Listen()
	if err := a.Start(); err != nil {
		t.Errorf("unexpected Start error, %s\n", err)
	}
	m.Stop()
	if mockWriter.msgCount != listenTestData.InsertCount {
		t.Errorf("unexpected message count, expected %d, got %d\n", listenTestData.InsertCount, mockWriter.msgCount)
	}
}

var stopTests = []struct {
	config      adaptor.Config // input
	expectedErr error          // expected error
}{
	{
		adaptor.Config{"uri": "mongodb://localhost:27017", "namespace": "test.test"},
		nil,
	},
	{
		adaptor.Config{"uri": "mongodb://localhost:27017", "namespace": "test.test", "bulk": true},
		nil,
	},
}

func TestStop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping MongoDB Stop in short mode")
	}

	for _, st := range stopTests {
		a, err := adaptor.CreateAdaptor(
			"mongodb",
			"test",
			st.config,
			pipe.NewPipe(nil, "test"),
		)
		if err != nil {
			t.Fatalf("unable to create MongoDB Adaptor, %s\n", err)
		}
		a.Listen()
		if err := a.Stop(); err != nil {
			t.Errorf("unable to Stop adaptor, %s\n", err)
		}

	}
}
