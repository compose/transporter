// +build integration

package transporter

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/adaptor"
)

var (
	metricsEvents = make([][]byte, 0)
)

type EventHolder struct {
	rawEvents [][]byte
}

func TestEventsBroadcast(t *testing.T) {
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

	// set up the nodes
	dummyOutNode := NewNode("dummyFileOut", "file", adaptor.Config{"uri": "file://" + outFile})
	dummyOutNode.Add(NewNode("dummyFileIn", "file", adaptor.Config{"uri": "file://" + inFile}))

	p, err := NewDefaultPipeline(dummyOutNode, ts.URL, "asdf", "jklm", 1*time.Second)
	if err != nil {
		t.Errorf("can't create pipeline, got %s", err.Error())
		t.FailNow()
	}

	err = p.Run()
	if err != nil {
		t.FailNow()
	}

	time.Sleep(time.Duration(5) * time.Second)

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
	var evt map[string]interface{}
	for _, val := range events.rawEvents {
		if err := json.Unmarshal(val, &evt); err != nil {
			return err
		}
		if evt["name"].(string) == metric {
			// check for path if provided
			if path != "" && evt["path"].(string) == path {
				return nil
			}
			return nil
		}
	}
	return fmt.Errorf("unable to locate metric, %s, in received metric events", metric)
}
