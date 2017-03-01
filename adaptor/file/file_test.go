package file

import (
	"fmt"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/pipe"
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
		mockPipe := &pipe.Mock{}
		a, err := adaptor.CreateAdaptor(
			"file",
			"test",
			st.config,
			sourcePipe,
		)
		if err != nil {
			t.Fatalf("unable to create File Adaptor, %s\n", err)
		}
		go sinkPipe.Listen(mockPipe.WriteMessage, regexp.MustCompile(".*"))
		if err := a.Start(); err != nil {
			t.Errorf("unexpected Start error, %s\n", err)
		}
		sinkPipe.Stop()
		if mockPipe.MsgCount != 10 {
			t.Errorf("unexpected message count, expected %d, got %d\n", 10, mockPipe.MsgCount)
		}
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
	mockWriter := &client.MockWriter{}
	f := &File{
		uri:         "",
		writer:      mockWriter,
		client:      &client.Mock{},
		path:        "test/listen",
		pipe:        sinkPipe,
		doneChannel: make(chan struct{}),
	}
	go f.Listen()
	if err := a.Start(); err != nil {
		t.Errorf("unexpected Start error, %s\n", err)
	}
	f.Stop()
	if mockWriter.MsgCount != 2 {
		t.Errorf("unexpected message count, expected %d, got %d\n", 2, mockWriter.MsgCount)
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
