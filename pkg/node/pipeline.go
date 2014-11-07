package node

/*
 * A Pipeline is a the end to end description of a transporter data flow.
 * including the source, sink, and all the transformers along the way
 */

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/robertkrimen/otto"
)

const (
	VERSION = "0.0.1"
)

type Pipeline struct {
	Source       *Node          `json:"source"`
	Sink         *Node          `json:"sink"`
	Transformers []*Transformer `json:"transformers"`
	errChan      chan error
	eventChan    chan Event
}

func NewPipeline(source *Node) *Pipeline {
	return &Pipeline{Source: source, Transformers: make([]*Transformer, 0)}
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
 * Create the pipeline, and instantiate all the nodes
 */
func (p *Pipeline) Create() error {
	err := p.Source.Create(SOURCE)
	if err != nil {
		return err
	}

	err = p.Sink.Create(SINK)
	if err != nil {
		return err
	}

	return nil
}

/*
 * run the pipeline
 */
func (p *Pipeline) Run() error {
	sourcePipe := NewPipe(p.Source.Name)
	p.errChan = sourcePipe.Err
	p.eventChan = sourcePipe.Event
	sinkPipe := JoinPipe(sourcePipe, p.Sink.Name)

	go p.startErrorListener()
	go p.startEventListener()

	// send a boot event
	p.eventChan <- NewBootEvent(time.Now().Unix(), VERSION, p.endpointMap())

	// TODO, this sucks because returning an error from the sink doesn't break the chain
	go p.Sink.NodeImpl.Start(sinkPipe)
	return p.Source.NodeImpl.Start(sourcePipe)
}

func (p *Pipeline) endpointMap() map[string]string {
	m := make(map[string]string)
	m[p.Source.Name] = p.Source.Type
	m[p.Sink.Name] = p.Sink.Type
	for _, v := range p.Transformers {
		m[v.Name] = "transformer"
	}
	return m
}

func (p *Pipeline) startErrorListener() {
	for {
		select {
		case err := <-p.errChan:
			fmt.Printf("Pipeline error %v\n", err)
		}
	}
}

func (p *Pipeline) startEventListener() {
	for {
		select {
		case event := <-p.eventChan:
			fmt.Printf("Pipeline event: %s\n", event)
		}
	}
}
