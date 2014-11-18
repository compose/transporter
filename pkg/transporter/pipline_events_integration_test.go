// +build integration

package transporter

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

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

	events := make([][]byte, 0)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		event, _ := ioutil.ReadAll(r.Body)
		r.Body.Close()
		events = append(events, event)
	}))
	defer ts.Close()
	ts.Start()

	var (
		eventApiConfig = Api{
			Uri:             ts.URL,
			MetricsInterval: 1000,
		}
		dummyOutConfig = ConfigNode{
			Extra: map[string]interface{}{"uri": "file:///tmp/dummyFileOut"},
			Name:  "dummyFileOut",
			Type:  "file",
		}
		dummyInConfig = ConfigNode{
			Extra: map[string]interface{}{"uri": "file:///tmp/dummyFileIn"},
			Name:  "dummyFileIn",
			Type:  "file",
		}
	)

	err := setupFileInAndOut(
		strings.Replace(dummyOutConfig.Extra["uri"].(string), "file://", "", 1),
		strings.Replace(dummyInConfig.Extra["uri"].(string), "file://", "", 1),
	)
	if err != nil {
		t.Errorf("can't create tmp files, got %s", err.Error())
		t.FailNow()
	}

	p, err := NewPipeline(dummyOutConfig, eventApiConfig)
	p.AddTerminalNode(dummyInConfig)
	if err != nil {
		t.Errorf("can't create pipeline, got %s", err.Error())
		t.FailNow()
	}

	p.Run()

	time.Sleep(time.Duration(5) * time.Second)

	if len(events) != 4 {
		t.Errorf("did not receive all events\nexp: %d\ngot: %d", 4, len(events))
	}

	var evt map[string]interface{}
	for idx, val := range data {
		if err = json.Unmarshal(events[idx], &evt); err != nil {
			t.Errorf("unable to unmarshal event, %s", err.Error())
		}
		if evt["event"].(string) != val.evt {
			t.Errorf("%s event should be %d event emitted, received %s\n", val.evt, idx, evt)
		}
		if val.evtPath != "" && evt["path"].(string) != val.evtPath {
			t.Errorf("%s path incorrect, received %s\n", val.evtPath, evt)
		}
	}

}
