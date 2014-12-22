package transporter

import (
	"errors"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/pipe"
)

var (
	fakesourceCN = NewNode("source1", "source", adaptor.Config{"value": "rockettes"})
	fileNode     = NewNode("localfile", "file", adaptor.Config{"uri": "file:///tmp/foo"})
)

// a noop node adaptor to help test
type Testadaptor struct {
	value string
}

func NewTestadaptor(p *pipe.Pipe, path string, extra adaptor.Config) (adaptor.StopStartListener, error) {
	val, ok := extra["value"]
	if !ok {
		return nil, errors.New("this is an error")
	}
	return &Testadaptor{value: val.(string)}, nil
}

func (s *Testadaptor) Stop() error {
	return nil
}

func (s *Testadaptor) Start() error {
	return nil
}

func (s *Testadaptor) Listen() error {
	return nil
}

func TestPipelineString(t *testing.T) {
	adaptor.Register("source", "description", NewTestadaptor, struct{}{})

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
			" - Source:         source1                                  source                                         \n  - Sink:          localfile                                file                                           file:///tmp/foo",
		},
	}

	for _, v := range data {
		if v.terminalNode != nil {
			v.in.Add(v.terminalNode)
		}
		p, err := NewDefaultPipeline(v.in, "", "", "", 100*time.Millisecond)
		if err != nil {
			t.Errorf("can't create pipeline, got %s", err.Error())
			t.FailNow()
		}
		if p.String() != v.out {
			t.Errorf("\nexpected:\n%s\ngot:\n%s\n", v.out, p.String())
		}
	}
}
