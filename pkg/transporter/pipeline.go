package transporter

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
	config Config
	nodes  []Node

	sourcePipe Pipe
	nodeWg     *sync.WaitGroup
	metricsWg  *sync.WaitGroup
}

func NewPipeline(config Config, nodes []ConfigNode) (*Pipeline, error) {
	p := &Pipeline{
		config:    config,
		nodes:     make([]Node, len(nodes)),
		nodeWg:    &sync.WaitGroup{},
		metricsWg: &sync.WaitGroup{},
	}

	var err error

	if len(nodes) < 2 {
		return nil, fmt.Errorf("pipeline needs at least 2 nodes, %d given", len(nodes))
	}

	for idx, n := range nodes {
		p.nodes[idx], err = n.Create()
		if err != nil {
			return nil, err
		}
	}

	p.sourcePipe = NewPipe(p.nodes[0].Config().Name, p.config)

	go p.startErrorListener()
	go p.startEventListener()

	return p, nil
}

func (p *Pipeline) String() string {
	out := " - Pipeline\n"
	out += fmt.Sprintf("  - Source: %s\n  - Sink:   %s\n  - Transformers:\n", p.nodes[0].Config(), p.nodes[len(p.nodes)-1].Config())
	for _, t := range p.nodes[1 : len(p.nodes)-1] {
		out += fmt.Sprintf("   - %s\n", t)
	}
	return out
}

func (p *Pipeline) stopEverything() {
	// stop all the nodes
	for _, node := range p.nodes {
		node.Stop()
	}
}

/*
 * run the pipeline
 */
func (p *Pipeline) Run() error {

	var pipe Pipe = p.sourcePipe

	for idx, node := range p.nodes[1:] {
		// lets get a joinPipe, unless we're the last one, and then lets use a terminalPipe
		if idx == len(p.nodes)-2 {
			pipe = TerminalPipe(pipe, node.Config().Name, p.config)
		} else {
			pipe = JoinPipe(pipe, node.Config().Name, p.config)
		}

		go func(pipe Pipe, node Node) {
			p.nodeWg.Add(1)
			node.Start(pipe)
			p.nodeWg.Done()
		}(pipe, node)
	}

	// send a boot event
	p.sourcePipe.Event <- NewBootEvent(time.Now().Unix(), VERSION, p.endpointMap())

	// start the source
	err := p.nodes[0].Start(p.sourcePipe)

	// the source has exited, stop all the other nodes
	p.stopEverything()

	// use the waitgroups and wait for nodes to exit
	p.nodeWg.Wait()
	p.metricsWg.Wait()

	return err
}

func (p *Pipeline) endpointMap() map[string]string {
	m := make(map[string]string)

	for _, v := range p.nodes {
		_, is_transformer := v.(*Transformer)
		if is_transformer {
			m[v.Config().Name] = "transformer"
		} else {
			m[v.Config().Name] = v.Config().Type
		}
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
			resp, err := http.Post(p.config.Api.Uri, "application/json", bytes.NewBuffer(ba))
			if err != nil {
				fmt.Println("event send failed")
				p.sourcePipe.Err <- err
				return
			}

			if resp.StatusCode != 200 {
				resp.Body.Close()
				p.sourcePipe.Err <- fmt.Errorf("Event Error: http error code, expected 200, got %d.  %d", resp.StatusCode, resp.StatusCode)
				return
			}
			resp.Body.Close()
		}()
		fmt.Printf("sent pipeline event: %s -> %s\n", p.config.Api.Uri, event)

	}
}
