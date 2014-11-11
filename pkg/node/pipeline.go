package node

/*
 * A Pipeline is a the end to end description of a transporter data flow.
 * including the source, sink, and all the transformers along the way
 */

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/robertkrimen/otto"
)

const (
	VERSION = "0.0.1"
)

type Pipeline struct {
	Config       Config
	Source       *Node          `json:"source"`
	Sink         *Node          `json:"sink"`
	Transformers []*Transformer `json:"transformers"`

	sourcePipe Pipe
	nodeWg     *sync.WaitGroup
	metricsWg  *sync.WaitGroup
}

func NewPipeline(source *Node, config Config) *Pipeline {
	p := &Pipeline{Source: source, Transformers: make([]*Transformer, 0), Config: config}

	return p
}

/*
 * create a new pipeline from a value, such as what we would get back
 * from an otto.Value.  basically a pipeline that has lost it's identify,
 * and been interfaced{}
 * TODO this should probably live in the javascript builder somewhere
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
 * TODO this should probably live in the javascript builder somewhere
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

func (p *Pipeline) init() {
	// initialize these.  all the marshalling and unmarshalling we've done
	// means these might not be instantiated properly
	p.sourcePipe = NewPipe(p.Source.Name, p.Config)
	p.nodeWg = &sync.WaitGroup{}
	p.metricsWg = &sync.WaitGroup{}

	go p.startErrorListener()
	go p.startEventListener()
}

func (p *Pipeline) startTransformers() (pipe Pipe) {
	pipe = p.sourcePipe
	for _, transformer := range p.Transformers {
		pipe = JoinPipe(pipe, transformer.Name, p.Config) // make a joinpipe
		go func(pipe Pipe, transformer *Transformer) {
			p.nodeWg.Add(1)
			transformer.Start(pipe)
			p.nodeWg.Done()
		}(pipe, transformer)
	}
	return pipe
}

func (p *Pipeline) startSink(pipe Pipe) {
	go func() {
		p.nodeWg.Add(1)
		p.Sink.NodeImpl.Start(pipe)
		p.nodeWg.Done()
	}()
}

func (p *Pipeline) stopEverything() {
	// stop all the nodes
	for _, transformer := range p.Transformers {
		transformer.Stop()
	}
	p.Sink.NodeImpl.Stop()
	p.Source.NodeImpl.Stop()
}

/*
 * run the pipeline
 */
func (p *Pipeline) Run() error {
	p.init()

	pipe := p.startTransformers()

	// start the sink
	p.startSink(JoinPipe(pipe, p.Sink.Name, p.Config))

	// send a boot event
	p.sourcePipe.Event <- NewBootEvent(time.Now().Unix(), VERSION, p.endpointMap())

	// start the source
	err := p.Source.NodeImpl.Start(p.sourcePipe)

	// the source has exited, stop all the other nodes
	p.stopEverything()

	// use the waitgroups and wait for nodes to exit
	p.nodeWg.Wait()
	p.metricsWg.Wait()

	return err
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
	for err := range p.sourcePipe.Err {
		fmt.Printf("Pipeline error %v\nShutting down pipeline\n", err)
		p.Source.NodeImpl.Stop()
		for _, transformer := range p.Transformers {
			transformer.Stop()
		}
		p.Sink.NodeImpl.Stop()
	}
}

func (p *Pipeline) startEventListener() {
	for event := range p.sourcePipe.Event {
		ba, err := json.Marshal(event)
		if err != err {
			p.sourcePipe.Err <- err
			continue
		}
		p.metricsWg.Add(1)
		go func() {
			defer p.metricsWg.Done()
			resp, err := http.Post(p.Config.Api.Uri, "application/json", bytes.NewBuffer(ba))
			if err != nil {
				p.sourcePipe.Err <- err
				return
			}

			if resp.StatusCode != 200 {
				resp.Body.Close()
				p.sourcePipe.Err <- fmt.Errorf("http error code, expected 200, got %d.  %d", resp.StatusCode, resp.StatusCode)
				return
			}
			resp.Body.Close()
		}()
		fmt.Printf("sent pipeline event: %s -> %s\n", p.Config.Api.Uri, event)

	}
}
