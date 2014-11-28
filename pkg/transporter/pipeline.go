package transporter

import (
	"fmt"
	"time"

	"github.com/compose/transporter/pkg/events"
)

const (
	VERSION = "0.0.1"
)

// A Pipeline is a the end to end description of a transporter data flow.
// including the source, sink, and all the transformers along the way

type Pipeline struct {
	source  *Node
	emitter events.Emitter
}

func NewDefaultPipeline(source *Node, api events.Api) (*Pipeline, error) {
	emitter := events.NewHttpPostEmitter(api)
	return NewPipeline(source, emitter, time.Duration(api.MetricsInterval)*time.Millisecond)
}

// NewPipeline creates a new Transporter Pipeline using the given tree of nodes
func NewPipeline(source *Node, emitter events.Emitter, interval time.Duration) (*Pipeline, error) {
	pipeline := &Pipeline{
		source:  source,
		emitter: emitter,
	}

	// init the pipeline
	err := pipeline.source.Init(interval)
	if err != nil {
		return pipeline, err
	}

	// init the emitter with the right chan
	pipeline.emitter.Init(source.pipe.Event)

	// start the emitters
	go pipeline.startErrorListener(source.pipe.Err)
	pipeline.emitter.Start()

	return pipeline, nil
}

func (pipeline *Pipeline) String() string {
	out := pipeline.source.String()
	return out
}

// Stop sends a stop signal to the emitter and all the nodes, whether they are running or not
func (pipeline *Pipeline) Stop() {
	pipeline.source.Stop()
	pipeline.emitter.Stop()
}

// run the pipeline
func (pipeline *Pipeline) Run() error {
	endpoints := pipeline.source.Endpoints()

	// send a boot event
	pipeline.source.pipe.Event <- events.NewBootEvent(time.Now().Unix(), VERSION, endpoints)

	// start the source
	err := pipeline.source.Start()

	// pipeline has stopped, send the exit event
	pipeline.source.pipe.Event <- events.NewExitEvent(time.Now().Unix(), VERSION, endpoints)

	// the source has exited, stop all the other nodes
	pipeline.Stop()

	// send a boot event

	return err
}

// start error listener consumes all the events on the pipe's Err channel, and stops the pipeline
// when it receives one
func (pipeline *Pipeline) startErrorListener(cherr chan error) {
	for err := range cherr {
		fmt.Printf("Pipeline error %v\nShutting down pipeline\n", err)
		pipeline.Stop()
	}
}
