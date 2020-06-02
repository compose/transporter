package pipeline

import (
	"time"

	"github.com/compose/transporter/events"
)

// A Pipeline is a the end to end description of a transporter data flow.
// including the source, sink, and all the transformers along the way
type Pipeline struct {
	source        *Node
	emitter       events.Emitter
	metricsTicker *time.Ticker
	version       string

	// Err is the fatal error that was sent from the adaptor
	// that caused us to stop this process.  If this is nil, then
	// the transporter is running
	Err  error
	done chan struct{}
}

// NewDefaultPipeline returns a new Transporter Pipeline with the given node tree, and
// uses the events.HttpPostEmitter to deliver metrics.
// eg.
//   a, err := adaptor.GetAdaptor("mongodb", map[string]interface{}{"uri": "mongo://localhost:27017"})
//   if err != nil {
//     fmt.Println(err)
//     os.Exit(1)
//   }
//   source := pipeline.NewNodeWithOptions(
//     "source", "mongo", "/.*/",
//     pipeline.WithClient(a),
//     pipeline.WithReader(a),
//   )
//   f, err := adaptor.GetAdaptor("file", map[string]interface{}{"uri": "stdout://"})
//   sink := pipeline.NewNodeWithOptions(
//     "out", "file", "/.*/",
//     pipeline.WithClient(f),
//     pipeline.WithWriter(f),
//     pipeline.WithParent(source),
//   )
//   pipeline, err := transporter.NewDefaultPipeline(source, events.Api{URI: "http://localhost/endpoint"}, 1*time.Second)
//   if err != nil {
// 	  fmt.Println(err)
// 	  os.Exit(1)
//   }
// pipeline.Run()
func NewDefaultPipeline(source *Node, uri, key, pid, version string, interval time.Duration) (*Pipeline, error) {
	return NewPipeline(version, source, events.HTTPPostEmitter(uri, key, pid), interval)
}

// NewPipeline creates a new Transporter Pipeline using the given tree of nodes, and Event Emitter
// eg.
//   a, err := adaptor.GetAdaptor("mongodb", map[string]interface{}{"uri": "mongo://localhost:27017"})
//   if err != nil {
//     fmt.Println(err)
//     os.Exit(1)
//   }
//   source := pipeline.NewNodeWithOptions(
//     "source", "mongo", "/.*/",
//     pipeline.WithClient(a),
//     pipeline.WithReader(a),
//   )
//   f, err := adaptor.GetAdaptor("file", map[string]interface{}{"uri": "stdout://"})
//   sink := pipeline.NewNodeWithOptions(
//     "out", "file", "/.*/",
//     pipeline.WithClient(f),
//     pipeline.WithWriter(f),
//     pipeline.WithParent(source),
//   )
//   pipeline, err := transporter.NewPipeline("version", source, events.NewNoopEmitter(), 1*time.Second)
//   if err != nil {
// 	  fmt.Println(err)
// 	  os.Exit(1)
//   }
// pipeline.Run()
func NewPipeline(version string, source *Node, emit events.EmitFunc, interval time.Duration) (*Pipeline, error) {

	pipeline := &Pipeline{
		source:        source,
		metricsTicker: time.NewTicker(interval),
		done:          make(chan struct{}),
	}

	// init the emitter with the right chan
	pipeline.emitter = events.NewEmitter(source.pipe.Event, emit)

	// start the emitters
	go pipeline.startMetricsGatherer()

	pipeline.emitter.Start()

	return pipeline, nil
}

func (pipeline *Pipeline) String() string {
	return pipeline.source.String()
}

// Stop sends a stop signal to the emitter and all the nodes, whether they are running or not.
// the node's database adaptors are expected to clean up after themselves, and stop will block until
// all nodes have stopped successfully
func (pipeline *Pipeline) Stop() {
	endpoints := pipeline.source.Endpoints()
	pipeline.source.Stop()

	// pipeline has stopped, emit one last round of metrics and send the exit event
	close(pipeline.done)
	pipeline.emitMetrics()
	pipeline.source.pipe.Event <- events.NewExitEvent(time.Now().UnixNano(), pipeline.version, endpoints)
	pipeline.emitter.Stop()
}

// Run the pipeline
func (pipeline *Pipeline) Run() error {
	endpoints := pipeline.source.Endpoints()
	// send a boot event
	pipeline.source.pipe.Event <- events.NewBootEvent(time.Now().UnixNano(), pipeline.version, endpoints)

	errors := make(chan error, 2)
	go func() {
		errors <- pipeline.startErrorListener()
	}()
	go func() {
		errors <- pipeline.source.Start()
	}()

	return <-errors
}

// start error listener consumes all the events on the pipe's Err channel, and stops the pipeline
// when it receives one
func (pipeline *Pipeline) startErrorListener() error {
	for {
		select {
		case err := <-pipeline.source.pipe.Err:
			return err
		case <-pipeline.done:
			return nil
		}
	}
}

func (pipeline *Pipeline) startMetricsGatherer() {
	for {
		select {
		case <-pipeline.metricsTicker.C:
			pipeline.emitMetrics()
		case <-pipeline.done:
			return
		}
	}
}

// emit the metrics
func (pipeline *Pipeline) emitMetrics() {
	pipeline.apply(func(node *Node) {
		pipeline.source.pipe.Event <- events.NewMetricsEvent(time.Now().UnixNano(), node.path, node.pipe.MessageCount)
	})
}

// apply maps a function f across all nodes of a pipeline
func (pipeline *Pipeline) apply(f func(*Node)) {
	head := pipeline.source
	nodes := []*Node{head}
	for len(nodes) > 0 {
		head, nodes = nodes[0], nodes[1:]
		f(head)
		nodes = append(nodes, head.children...)
	}
}
