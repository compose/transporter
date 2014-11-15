package transporter

import (
	"testing"
)

var (
	fakesourceCN = ConfigNode{Type: "source", Extra: map[string]interface{}{"value": "rockettes"}}
	fileCN       = ConfigNode{Extra: map[string]interface{}{"uri": "file:///tmp/crap"}, Name: "localfile", Type: "file"}
)

var (
	testConfig = Config{
		Api: Api{
			Uri:             "",
			MetricsInterval: 100,
		},
		Nodes: map[string]ConfigNode{
			"fakesource": fakesourceCN,
			"localfile":  fileCN,
		},
	}
)

func TestPipelineString(t *testing.T) {
	sourceRegistry["source"] = NewSourceImpl

	data := []struct {
		in           ConfigNode
		terminalNode *ConfigNode
		out          string
	}{
		{
			fakesourceCN,
			nil,
			" - Pipeline\n  - Source:                      source          no namespace set               no uri set\n",
		},
		{
			fakesourceCN,
			&fileCN,
			" - Pipeline\n  - Source:                      source          no namespace set               no uri set\n  - Sink:   localfile            file            no namespace set               file:///tmp/crap\n",
		},
	}

	for _, v := range data {
		p, err := NewPipeline(testConfig, v.in)
		if err != nil {
			t.Errorf("can't create pipeline, got %s", err.Error())
			t.FailNow()
		}
		if v.terminalNode != nil {
			p.AddTerminalNode(*v.terminalNode)
		}
		if p.String() != v.out {
			t.Errorf("\nexp %s\ngot %s", v.out, p.String())
		}
	}
}
