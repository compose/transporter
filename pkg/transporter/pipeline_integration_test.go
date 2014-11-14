// +build integration

package transporter

import (
	"os"
	"strings"
	"testing"
	// "github.com/compose/transporter/pkg/pipe"
)

var (
	integrationFileOutCN = ConfigNode{Extra: map[string]interface{}{"uri": "file:///tmp/crapOut"}, Name: "localfile", Type: "file"}
	integrationFileInCN  = ConfigNode{Extra: map[string]interface{}{"uri": "file:///tmp/crapIn"}, Name: "localfile", Type: "file"}
)

var (
	testIntegrationConfig = Config{
		Api: Api{
			Uri:             "http://requestb.in/qhqfuzqh",
			MetricsInterval: 100,
		},
		Nodes: map[string]ConfigNode{
			"localmongo": integrationFileOutCN,
			"localfile":  integrationFileInCN,
		},
	}
)

func TestPipelineRun(t *testing.T) {
	filenameOut := strings.Replace(integrationFileOutCN.Extra["uri"].(string), "file://", "", 1)
	err := os.Remove(filenameOut)
	if err != nil && !strings.Contains(err.Error(), "no such file or directory") {
		t.Errorf("unable to remove tmp file, got %s", err.Error())
	}
	filenameIn := strings.Replace(integrationFileInCN.Extra["uri"].(string), "file://", "", 1)
	err = os.Remove(filenameIn)
	if err != nil {
		t.Errorf("unable to remove tmp file, got %s", err.Error())
	}
	inFileOut, err := os.Create(filenameOut)
	if err != nil {
		t.Errorf("unable to open tmp file, got %s", err.Error())
	}
	inFileOut.WriteString("{\"_id\":\"546656989330a846dc7ce327\",\"test\":\"hello world\"}\n")
	inFileOut.Close()
	data := []struct {
		in           ConfigNode
		terminalNode *ConfigNode
	}{
		{
			integrationFileOutCN,
			&integrationFileInCN,
		},
	}

	for _, v := range data {
		p, err := NewPipeline(testIntegrationConfig, v.in)
		if err != nil {
			t.Errorf("can't create pipeline, got %s", err.Error())
			t.FailNow()
		}
		if v.terminalNode != nil {
			p.AddTerminalNode(*v.terminalNode)
		}

		err = p.Run()
		if err != nil {
			t.Errorf("error running pipeline, got %s", err.Error())
			t.FailNow()
		}

		sourceFile, _ := os.Open(filenameOut)
		sourceSize, _ := sourceFile.Stat()
		defer sourceFile.Close()
		sinkFile, _ := os.Open(filenameIn)
		sinkSize, _ := sinkFile.Stat()
		defer sinkFile.Close()
		if sourceSize.Size() != sinkSize.Size() {
			t.Errorf("Incorrect file size\nexp %d\ngot %d", sourceSize.Size(), sinkSize.Size())
		}

	}

}
