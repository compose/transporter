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

func NewPipeline(source *Node, sink *Node, config Config, transformers []*Transformer) *Pipeline {
	p := &Pipeline{
		Source:       source,
		Sink:         sink,
		Transformers: transformers,
		Config:       config,
		sourcePipe:   NewPipe(source.Name, config),
		nodeWg:       &sync.WaitGroup{},
		metricsWg:    &sync.WaitGroup{},
	}

	return p
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
		p.stopEverything()
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
