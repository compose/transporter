package pipeline

import (
	"testing"
	"time"

	"github.com/compose/transporter/adaptor"
	_ "github.com/compose/transporter/adaptor/file"
)

var (
	fakesourceCN = NewNode("source1", "source", adaptor.Config{"value": "rockettes", "namespace": "a./.*/"})
	fileNode     = NewNode("localfile", "file", adaptor.Config{"uri": "file:///tmp/foo", "namespace": "a./.*/"})
)

// a noop node adaptor to help test
type Testadaptor struct {
	value string
}

func init() {
	adaptor.Add(
		"source",
		func() adaptor.Adaptor {
			return &adaptor.Mock{}
		},
	)
}

func (s *Testadaptor) Description() string {
	return "description"
}

func (s *Testadaptor) SampleConfig() string {
	return ""
}

func TestPipelineString(t *testing.T) {
	data := []struct {
		in           *Node
		terminalNode *Node
		out          string
	}{
		{
			fakesourceCN,
			nil,
			" - Source:         source1                                  source          a./.*/                         ",
		},
		{
			fakesourceCN,
			fileNode,
			" - Source:         source1                                  source          a./.*/                         \n  - Sink:          localfile                                file            a./.*/                         file:///tmp/foo",
		},
	}

	for _, v := range data {
		if v.terminalNode != nil {
			v.in.Add(v.terminalNode)
		}
		p, err := NewDefaultPipeline(v.in, "", "", "", "test", 100*time.Millisecond)
		if err != nil {
			t.Errorf("can't create pipeline, got %s", err.Error())
			t.FailNow()
		}
		if p.String() != v.out {
			t.Errorf("\nexpected:\n%s\ngot:\n%s\n", v.out, p.String())
		}
	}
}
