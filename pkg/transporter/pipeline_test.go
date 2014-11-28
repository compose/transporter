package transporter

import (
	"errors"
	"testing"

	"github.com/compose/transporter/pkg/events"
	"github.com/compose/transporter/pkg/impl"
	"github.com/compose/transporter/pkg/pipe"
)

var (
	fakesourceCN = NewNode("source1", "source", map[string]interface{}{"value": "rockettes"})
	fileNode     = NewNode("localfile", "file", map[string]interface{}{"uri": "file:///tmp/crap"})
)

var (
	testEmptyApiConfig = events.Api{
		Uri:             "",
		MetricsInterval: 100,
	}
)

// a noop node impl to help test
type TestImpl struct {
	value string
}

func NewTestImpl(p *pipe.Pipe, extra map[string]interface{}) (*TestImpl, error) {
	val, ok := extra["value"]
	if !ok {
		return nil, errors.New("this is an error")
	}
	return &TestImpl{value: val.(string)}, nil
}

func (s *TestImpl) Stop() error {
	return nil
}

func (s *TestImpl) Start() error {
	return nil
}

func (s *TestImpl) Listen() error {
	return nil
}

func TestPipelineString(t *testing.T) {
	impl.Registry["source"] = NewTestImpl

	data := []struct {
		in           *Node
		terminalNode *Node
		out          string
	}{
		{
			fakesourceCN,
			nil,
			" - Source:         source1                                  source                                         ",
		},
		{
			fakesourceCN,
			fileNode,
			" - Source:         source1                                  source                                         \n  - Sink:          localfile                                file                                           file:///tmp/crap",
		},
	}

	for _, v := range data {
		if v.terminalNode != nil {
			v.in.Add(v.terminalNode)
		}
		p, err := NewDefaultPipeline(v.in, testEmptyApiConfig)
		if err != nil {
			t.Errorf("can't create pipeline, got %s", err.Error())
			t.FailNow()
		}
		if p.String() != v.out {
			t.Errorf("\nexpected:\n%s\ngot:\n%s\n", v.out, p.String())
		}
	}
}
