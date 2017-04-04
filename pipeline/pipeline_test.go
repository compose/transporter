package pipeline

import (
	"regexp"
	"testing"
	"time"

	"github.com/compose/transporter/adaptor"
	_ "github.com/compose/transporter/adaptor/file"
	"github.com/compose/transporter/pipe"
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
			&Node{Name: "source1", Type: "source", nsFilter: regexp.MustCompile(".*"), pipe: pipe.NewPipe(nil, "source1")},
			&Node{Name: "localfile", Type: "file", nsFilter: regexp.MustCompile(".*")},
			` - Source:         source1                                  source          .*                            
  - Sink:          localfile                                file            .*                            `,
		},
	}

	for _, v := range data {
		if v.terminalNode != nil {
			v.terminalNode.Parent = v.in
			v.terminalNode.pipe = pipe.NewPipe(v.in.pipe, "localfile")
			v.in.Children = []*Node{v.terminalNode}
		}
		p, err := NewDefaultPipeline(v.in, "", "", "", "test", 100*time.Millisecond)
		if err != nil {
			t.Errorf("can't create pipeline, got %s", err.Error())
			t.FailNow()
		}
		actual := p.String()
		if actual != v.out {
			t.Errorf("\nexpected:\n%v\ngot:\n%v\n", v.out, actual)
		}

		close(p.source.pipe.Err)
	}
}
