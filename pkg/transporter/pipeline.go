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

	"github.com/compose/transporter/pkg/pipe"
)

const (
	VERSION = "0.0.1"
)

type Pipeline struct {
	config Config

	source pipelineSource
	chunks []pipelineChunk

	nodeWg    *sync.WaitGroup
	metricsWg *sync.WaitGroup
}

// NewPipeline creates a new Transporter Pipeline, with the given node acting as the 'SOURCE'.  subsequent nodes should be added via AddNode
func NewPipeline(config Config, source ConfigNode) (*Pipeline, error) {
	p := &Pipeline{
		config:    config,
		chunks:    make([]pipelineChunk, 0),
		nodeWg:    &sync.WaitGroup{},
		metricsWg: &sync.WaitGroup{},
	}

	sourcePipe := pipe.NewSourcePipe(source.Name, time.Duration(p.config.Api.MetricsInterval)*time.Millisecond)
	node, err := source.CreateSource(sourcePipe)
	if err != nil {
		return p, err
	}

	p.source = pipelineSource{config: source, node: node, p: sourcePipe}

	go p.startErrorListener(sourcePipe.Err)
	go p.startEventListener(sourcePipe.Event)

	return p, nil
}

func (p *Pipeline) lastPipe() pipe.Pipe {
	if len(p.chunks) == 0 {
		return p.source.p
	}
	return p.chunks[len(p.chunks)-1].p
}

// AddNode adds a node to the pipeline
func (p *Pipeline) AddNode(config ConfigNode) error {
	return p.addNode(config, pipe.NewJoinPipe(p.lastPipe(), config.Name))
}

func (p *Pipeline) AddTerminalNode(config ConfigNode) error {
	return p.addNode(config, pipe.NewSinkPipe(p.lastPipe(), config.Name))
}

func (p *Pipeline) addNode(config ConfigNode, pp pipe.Pipe) error {
	node, err := config.Create(pp)
	if err != nil {
		return err
	}
	n := pipelineChunk{config: config, node: node, p: pp}
	p.chunks = append(p.chunks, n)
	return nil
}

func (p *Pipeline) String() string {
	out := " - Pipeline\n"
	out += fmt.Sprintf("  - Source: %s\n", p.source.config)
	if len(p.chunks) > 1 {
		for _, t := range p.chunks[1 : len(p.chunks)-1] {
			out += fmt.Sprintf("   - %s\n", t)
		}
	}
	if len(p.chunks) >= 1 {
		out += fmt.Sprintf("  - Sink:   %s\n", p.chunks[len(p.chunks)-1].config)
	}
	return out
}

func (p *Pipeline) stopEverything() {
	// stop all the nodes
	p.source.node.Stop()
	for _, chunk := range p.chunks {
		chunk.node.Stop()
	}
}

/*
 * run the pipeline
 */
func (p *Pipeline) Run() error {
	for _, chunk := range p.chunks {
		go func(node Node) {
			p.nodeWg.Add(1)
			node.Listen()
			p.nodeWg.Done()
		}(chunk.node)
	}

	// send a boot event
	p.source.p.Event <- pipe.NewBootEvent(time.Now().Unix(), VERSION, p.endpointMap())

	// start the source
	err := p.source.node.Start()

	// the source has exited, stop all the other nodes
	p.stopEverything()

	p.nodeWg.Wait()
	p.metricsWg.Wait()

	return err
}

func (p *Pipeline) endpointMap() map[string]string {
	m := make(map[string]string)

	for _, v := range p.chunks {
		m[v.config.Name] = v.config.Type
	}
	return m
}

func (p *Pipeline) startErrorListener(cherr chan error) {
	for err := range cherr {
		fmt.Printf("Pipeline error %v\nShutting down pipeline\n", err)
		p.stopEverything()
	}
}

func (p *Pipeline) startEventListener(chevent chan pipe.Event) {
	for event := range chevent {
		ba, err := json.Marshal(event)
		if err != err {
			p.source.p.Err <- err
			continue
		}
		p.metricsWg.Add(1)
		go func() {
			defer p.metricsWg.Done()
			resp, err := http.Post(p.config.Api.Uri, "application/json", bytes.NewBuffer(ba))
			if err != nil {
				fmt.Println("event send failed")
				p.source.p.Err <- err
				return
			}

			if resp.StatusCode != 200 {
				resp.Body.Close()
				p.source.p.Err <- fmt.Errorf("Event Error: http error code, expected 200, got %d.  %d", resp.StatusCode, resp.StatusCode)
				return
			}
			resp.Body.Close()
		}()
		fmt.Printf("sent pipeline event: %s -> %s\n", p.config.Api.Uri, event)

	}
}

// pipelineChunk keeps a copy of the config beside the actual node implementation, so that we don't have to force fit the properties of the config
// into nodes that don't / shouldn't care about them.
type pipelineChunk struct {
	config ConfigNode
	node   Node
	p      pipe.Pipe
}

type pipelineSource struct {
	config ConfigNode
	node   Source
	p      pipe.Pipe
}
