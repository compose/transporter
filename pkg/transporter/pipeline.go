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
	pipeline := &Pipeline{
		config:    config,
		chunks:    make([]pipelineChunk, 0),
		nodeWg:    &sync.WaitGroup{},
		metricsWg: &sync.WaitGroup{},
	}

	sourcePipe := pipe.NewSourcePipe(source.Name, time.Duration(pipeline.config.Api.MetricsInterval)*time.Millisecond)
	node, err := source.CreateSource(sourcePipe)
	if err != nil {
		return pipeline, err
	}

	pipeline.source = pipelineSource{config: source, node: node, pipe: sourcePipe}

	go pipeline.startErrorListener(sourcePipe.Err)
	go pipeline.startEventListener(sourcePipe.Event)

	return pipeline, nil
}

func (pipeline *Pipeline) lastPipe() pipe.Pipe {
	if len(pipeline.chunks) == 0 {
		return pipeline.source.pipe
	}
	return pipeline.chunks[len(pipeline.chunks)-1].pipe
}

// AddNode adds a node to the pipeline
func (pipeline *Pipeline) AddNode(config ConfigNode) error {
	return pipeline.addNode(config, pipe.NewJoinPipe(pipeline.lastPipe(), config.Name))
}

func (pipeline *Pipeline) AddTerminalNode(config ConfigNode) error {
	return pipeline.addNode(config, pipe.NewSinkPipe(pipeline.lastPipe(), config.Name))
}

func (pipeline *Pipeline) addNode(config ConfigNode, p pipe.Pipe) error {
	node, err := config.Create(p)
	if err != nil {
		return err
	}
	n := pipelineChunk{config: config, node: node, pipe: p}
	pipeline.chunks = append(pipeline.chunks, n)
	return nil
}

func (pipeline *Pipeline) String() string {
	out := " - Pipeline\n"
	out += fmt.Sprintf("  - Source: %s\n", pipeline.source.config)
	if len(pipeline.chunks) > 1 {
		for _, t := range pipeline.chunks[1 : len(pipeline.chunks)-1] {
			out += fmt.Sprintf("   - %s\n", t)
		}
	}
	if len(pipeline.chunks) >= 1 {
		out += fmt.Sprintf("  - Sink:   %s\n", pipeline.chunks[len(pipeline.chunks)-1].config)
	}
	return out
}

func (pipeline *Pipeline) stopEverything() {
	// stop all the nodes
	pipeline.source.node.Stop()
	for _, chunk := range pipeline.chunks {
		chunk.node.Stop()
	}
}

/*
 * run the pipeline
 */
func (pipeline *Pipeline) Run() error {
	for _, chunk := range pipeline.chunks {
		go func(node Node) {
			pipeline.nodeWg.Add(1)
			node.Listen()
			pipeline.nodeWg.Done()
		}(chunk.node)
	}

	// send a boot event
	pipeline.source.pipe.Event <- pipe.NewBootEvent(time.Now().Unix(), VERSION, pipeline.endpointMap())

	// start the source
	err := pipeline.source.node.Start()

	// the source has exited, stop all the other nodes
	pipeline.stopEverything()

	pipeline.nodeWg.Wait()
	pipeline.metricsWg.Wait()

	return err
}

func (pipeline *Pipeline) endpointMap() map[string]string {
	m := make(map[string]string)
	m[pipeline.source.config.Name] = pipeline.source.config.Type
	for _, v := range pipeline.chunks {
		m[v.config.Name] = v.config.Type
	}
	return m
}

func (pipeline *Pipeline) startErrorListener(cherr chan error) {
	for err := range cherr {
		fmt.Printf("Pipeline error %v\nShutting down pipeline\n", err)
		pipeline.stopEverything()
	}
}

func (pipeline *Pipeline) startEventListener(chevent chan pipe.Event) {
	for event := range chevent {
		ba, err := json.Marshal(event)
		if err != err {
			pipeline.source.pipe.Err <- err
			continue
		}
		pipeline.metricsWg.Add(1)
		go func() {
			defer pipeline.metricsWg.Done()
			resp, err := http.Post(pipeline.config.Api.Uri, "application/json", bytes.NewBuffer(ba))
			if err != nil {
				fmt.Println("event send failed")
				pipeline.source.pipe.Err <- err
				return
			}

			if resp.StatusCode != 200 {
				resp.Body.Close()
				pipeline.source.pipe.Err <- fmt.Errorf("Event Error: http error code, expected 200, got %d.  %d", resp.StatusCode, resp.StatusCode)
				return
			}
			resp.Body.Close()
		}()
		fmt.Printf("sent pipeline event: %s -> %s\n", pipeline.config.Api.Uri, event)

	}
}

// pipelineChunk keeps a copy of the config beside the actual node implementation, so that we don't have to force fit the properties of the config
// into nodes that don't / shouldn't care about them.
type pipelineChunk struct {
	config ConfigNode
	node   Node
	pipe   pipe.Pipe
}

// pipelineSource is the source node, pipeline and config
type pipelineSource struct {
	config ConfigNode
	node   Source
	pipe   pipe.Pipe
}
