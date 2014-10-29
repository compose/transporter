package node

/*
 * A Pipeline is a the end to end description of a transporter data flow.
 * including the source, sink, and all the transformers along the way
 */

import (
	"encoding/json"
	"fmt"

	"github.com/robertkrimen/otto"
)

type Pipeline struct {
	Source       *Node          `json:"source"`
	Sink         *Node          `json:"sink"`
	Transformers []*Transformer `json:"transformers"`
}

func NewPipeline(source *Node) *Pipeline {
	return &Pipeline{Source: source, Transformers: make([]*Transformer, 0)}
}

/*
 * add a transformer function to a pipeline.
 * transformers will be called in fifo order
 */
func (p *Pipeline) AddTransformer(t *Transformer) {
	p.Transformers = append(p.Transformers, t)
}

func (p *Pipeline) String() string {
	out := " - Pipeline\n"
	out += fmt.Sprintf("  - Source: %s\n  - Sink:   %s\n  - Transformers:\n", p.Source, p.Sink)
	for _, t := range p.Transformers {
		out += fmt.Sprintf("   - %s\n", t)
	}
	return out
}

/*
 * create a new pipeline from a value, such as what we would get back
 * from an otto.Value.  basically a pipeline that has lost it's identify,
 * and been interfaced{}
 */
func InterfaceToPipeline(val interface{}) (Pipeline, error) {
	t := Pipeline{}
	ba, err := json.Marshal(val)

	if err != nil {
		return t, err
	}

	err = json.Unmarshal(ba, &t)
	return t, err
}

/*
 * turn this pipeline into an otto Object
 */
func (t *Pipeline) Object() (*otto.Object, error) {
	vm := otto.New()
	ba, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}

	return vm.Object(fmt.Sprintf(`(%s)`, string(ba)))
}
