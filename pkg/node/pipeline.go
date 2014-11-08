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
	errChan      chan error
	eventChan    chan Event
	stopChan     chan bool

	wg sync.WaitGroup
}

func NewPipeline(source *Node, config Config) *Pipeline {
	return &Pipeline{Source: source, Transformers: make([]*Transformer, 0), Config: config}
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
	defer p.wg.Wait()

	sourcePipe := NewPipe(p.Source.Name, p.Config)

	p.errChan = sourcePipe.Err
	p.eventChan = sourcePipe.Event

	sinkPipe := JoinPipe(sourcePipe, p.Sink.Name, p.Config)

	go p.startErrorListener()
	go p.startEventListener()

	// send a boot event
	p.eventChan <- NewBootEvent(time.Now().Unix(), VERSION, p.endpointMap())

	// TODO, this sucks because returning an error from the sink doesn't break the chain
	go p.Sink.NodeImpl.Start(sinkPipe)

	err := p.Source.NodeImpl.Start(sourcePipe)
	fmt.Println("source finished, sending stop")
	p.Sink.NodeImpl.Stop()
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
	for err := range p.errChan {
		fmt.Printf("Pipeline error %v\n", err)
	}
}

func (p *Pipeline) startEventListener() {
	for event := range p.eventChan {
		ba, err := json.Marshal(event)
		if err != err {
			p.errChan <- err
			continue
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			resp, err := http.Post(p.Config.Api.Uri, "application/json", bytes.NewBuffer(ba))
			if err != nil {
				p.errChan <- err
				return
			}

			if resp.StatusCode != 200 {
				resp.Body.Close()
				p.errChan <- fmt.Errorf("http error code, expected 200, got %d.  %s", resp.StatusCode, resp.StatusCode)
				return
			}
			resp.Body.Close()
		}()
		fmt.Printf("sent pipeline event: %s -> %s\n", p.Config.Api.Uri, event)

	}
}
