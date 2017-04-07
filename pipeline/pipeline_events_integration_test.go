package pipeline

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/offset"
)

var (
	metricsEvents = make([][]byte, 0)
)

type EventHolder struct {
	rawEvents [][]byte
}

func TestEventsBroadcast(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping EventsBroadcast in short mode")
	}
	data := []struct {
		evt     string
		evtPath string
	}{
		{
			"boot",
			"",
		},
		{
			"metrics",
			"dummyFileOut",
		},
		{
			"metrics",
			"dummyFileOut/dummyFileIn",
		},
		{
			"exit",
			"",
		},
	}

	eh := &EventHolder{rawEvents: make([][]byte, 0)}
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		event, _ := ioutil.ReadAll(r.Body)
		r.Body.Close()
		eh.rawEvents = append(eh.rawEvents, event)
	}))
	defer ts.Close()
	ts.Start()

	var (
		inFile  = "/tmp/dummyFileIn"
		outFile = "/tmp/dummyFileOut"
	)

	setupFiles(inFile, outFile)
	rand.Seed(time.Now().Unix())
	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("transporter%d", rand.Int31()))
	os.MkdirAll(dataDir, 0777)
	defer os.RemoveAll(dataDir)

	// set up the nodes
	f, err := adaptor.GetAdaptor("file", adaptor.Config{"uri": "file://" + outFile})
	if err != nil {
		t.Fatalf("can't create GetAdaptor, got %s", err)
	}
	dummyOutNode, err := NewNodeWithOptions(
		"dummyFileOut", "file", "/.*",
		WithClient(f),
		WithReader(f),
		WithCommitLog(dataDir, 1024*1024*1024),
	)
	if err != nil {
		t.Fatalf("can't create NewNode, got %s", err)
	}
	f, err = adaptor.GetAdaptor("file", adaptor.Config{"uri": "file://" + inFile})
	if err != nil {
		t.Fatalf("can't create GetAdaptor, got %s", err)
	}
	om, err := offset.NewLogManager(dataDir, "dummyFileIn")
	if err != nil {
		t.Fatalf("unexpected NewLogManager error, %s", err)
	}
	_, err = NewNodeWithOptions(
		"dummyFileIn", "file", "/.*/",
		WithParent(dummyOutNode),
		WithClient(f),
		WithWriter(f),
		WithOffsetManager(om),
	)
	if err != nil {
		t.Fatalf("can't create NewNode, got %s", err)
	}

	p, err := NewDefaultPipeline(dummyOutNode, ts.URL, "asdf", "jklm", "test", 1*time.Second)
	if err != nil {
		t.Errorf("can't create pipeline, got %s", err.Error())
		t.FailNow()
	}

	err = p.Run()
	if err != nil {
		t.FailNow()
	}

	p.Stop()

	time.Sleep(time.Duration(1) * time.Second)

	if len(eh.rawEvents) != 4 {
		t.Errorf("did not receive all events\nexp: %d\ngot: %d", 4, len(eh.rawEvents))
	}

	for _, val := range data {
		if err = eh.lookupMetricEvent(val.evt, val.evtPath); err != nil {
			t.Errorf("problem validating metric event, %s", err.Error())
		}
	}

}

func (events EventHolder) lookupMetricEvent(metric, path string) error {
	evt := struct {
		Name string `json:"name"`
		Path string `json:"path"`
	}{}
	for _, val := range events.rawEvents {
		if err := json.Unmarshal(val, &evt); err != nil {
			return err
		}
		if evt.Name == metric && (path == "" || path == evt.Path) {
			return nil
		}
	}
	return fmt.Errorf("unable to locate metric, %s (%s), in received metric events", metric, path)
}
