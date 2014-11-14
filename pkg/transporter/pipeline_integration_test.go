// +build integration

package transporter

import (
	"os"
	"reflect"
	"strings"
	"testing"
	// "github.com/compose/transporter/pkg/pipe"
)

var (
	localmongoCN         = ConfigNode{Extra: map[string]interface{}{"uri": "mongodb://localhost/blah", "namespace": "boo.baz"}, Name: "localmongo", Type: "mongo"}
	integrationFileOutCN = ConfigNode{Extra: map[string]interface{}{"uri": "file:///tmp/crapOut"}, Name: "localfileout", Type: "file"}
	integrationFileInCN  = ConfigNode{Extra: map[string]interface{}{"uri": "file:///tmp/crapIn"}, Name: "localfilein", Type: "file"}
)

var (
	testIntegrationConfig = Config{
		Api: Api{
			Uri:             "http://requestb.in/qhqfuzqh",
			MetricsInterval: 100,
		},
		Nodes: map[string]ConfigNode{
			"localfileout": integrationFileOutCN,
			"localfilein":  integrationFileInCN,
		},
	}
)

var (
	filenameOut = strings.Replace(integrationFileOutCN.Extra["uri"].(string), "file://", "", 1)
	filenameIn  = strings.Replace(integrationFileInCN.Extra["uri"].(string), "file://", "", 1)
)

func clearAndCreateFiles() (*os.File, error) {
	err := os.Remove(filenameOut)
	if err != nil && !strings.Contains(err.Error(), "no such file or directory") {
		return nil, err
	}
	err = os.Remove(filenameIn)
	if err != nil && !strings.Contains(err.Error(), "no such file or directory") {
		return nil, err
	}
	return os.Create(filenameOut)
}

func setupFileInAndOut() error {
	inFileOut, err := clearAndCreateFiles()
	if err != nil {
		return err
	}
	inFileOut.WriteString("{\"_id\":\"546656989330a846dc7ce327\",\"test\":\"hello world\"}\n")
	inFileOut.Close()
	return nil
}

func TestPipelineRun(t *testing.T) {
	data := []struct {
		setupFn      interface{}
		in           *ConfigNode
		transformer  []ConfigNode
		terminalNode *ConfigNode
	}{
		{
			setupFileInAndOut,
			&integrationFileOutCN,
			nil,
			&integrationFileInCN,
		},
	}

	for _, v := range data {
		if v.setupFn != nil {
			result := reflect.ValueOf(v.setupFn).Call(nil)
			if result[0].Interface() != nil {
				t.Errorf("unable to call setupFn, got %s", result[0].Interface().(error).Error())
				t.FailNow()
			}
		}
		p, err := NewPipeline(testIntegrationConfig, *v.in)
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
