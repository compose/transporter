package transporter

import (
	"testing"

	"github.com/compose/transporter/pkg/pipe"
)

var (
	// localmongoCN = ConfigNode{Extra: map[string]interface{}{"uri": "mongodb://localhost/blah", "namespace": "boo.baz"}, Name: "localmongo", Type: "mongo"}
	fakesourceCN = ConfigNode{Type: "fakesource", Extra: map[string]interface{}{"value": "rockettes"}}
	fileCN       = ConfigNode{Extra: map[string]interface{}{"uri": "file:///tmp/crap"}, Name: "locafile", Type: "file"}
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

// a random type that implements the source interface
type FakeSourceImpl struct {
	value string
}

func NewFakeSourceImpl(p pipe.Pipe, extra map[string]interface{}) (*FakeSourceImpl, error) {
	val, ok := extra["value"]
	if !ok {
		return nil, anError
	}
	return &FakeSourceImpl{value: val.(string)}, nil
}

func (s *FakeSourceImpl) Stop() error {
	return nil
}

func (s *FakeSourceImpl) Start() error {
	return nil
}

func TestPipelineString(t *testing.T) {
	SourceRegistry["fakesource"] = NewFakeSourceImpl

	data := []struct {
		in           ConfigNode
		terminalNode *ConfigNode
		out          string
	}{
		{
			fakesourceCN,
			nil,
			" - Pipeline\n  - Source:                      fakesource      no namespace set               no uri set\n",
		},
		{
			fakesourceCN,
			&fileCN,
			" - Pipeline\n  - Source:                      fakesource      no namespace set               no uri set\n  - Sink:   locafile             file            no namespace set               file:///tmp/crap\n",
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
