package transporter

import (
	"log"
	"time"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/events"
	"github.com/compose/transporter/pkg/state"
)

// VERSION the library
const (
	VERSION = "0.1.1"
)

// A Pipeline is a the end to end description of a transporter data flow.
// including the source, sink, and all the transformers along the way
type Pipeline struct {
	source        *Node
	emitter       events.Emitter
	sessionStore  state.SessionStore
	metricsTicker *time.Ticker

	// Err is the fatal error that was sent from the adaptor
	// that caused us to stop this process.  If this is nil, then
	// the transporter is running
	Err           error
	sessionTicker *time.Ticker
}

// NewDefaultPipeline returns a new Transporter Pipeline with the given node tree, and
// uses the events.HttpPostEmitter to deliver metrics.
// eg.
//   source :=
//   	transporter.NewNode("source", "mongo", adaptor.Config{"uri": "mongodb://localhost/", "namespace": "boom.foo", "debug": false, "tail": true}).
// 	  	Add(transporter.NewNode("out", "file", adaptor.Config{"uri": "stdout://"}))
//   pipeline, err := transporter.NewDefaultPipeline(source, events.Api{URI: "http://localhost/endpoint"}, 1*time.Second)
//   if err != nil {
// 	  fmt.Println(err)
// 	  os.Exit(1)
//   }
// pipeline.Run()
func NewDefaultPipeline(source *Node, uri, key, pid string, interval time.Duration) (*Pipeline, error) {
	emitter := events.NewHTTPPostEmitter(uri, key, pid)
	return NewPipeline(source, emitter, interval, nil, 10*time.Second)
}

// NewPipeline creates a new Transporter Pipeline using the given tree of nodes, and Event Emitter
// eg.
//   source :=
//   	transporter.NewNode("source", "mongo", adaptor.Config{"uri": "mongodb://localhost/", "namespace": "boom.foo", "debug": false, "tail": true}).
// 	  	Add(transporter.NewNode("out", "file", adaptor.Config{"uri": "stdout://"}))
//   pipeline, err := transporter.NewPipeline(source, events.NewNoopEmitter(), 1*time.Second, state.NewFilestore(pid, "/tmp/transporter.state"), 10*time.Second)
//   if err != nil {
// 	  fmt.Println(err)
// 	  os.Exit(1)
//   }
// pipeline.Run()
func NewPipeline(source *Node, emitter events.Emitter, interval time.Duration, sessionStore state.SessionStore, sessionInterval time.Duration) (*Pipeline, error) {
	pipeline := &Pipeline{
		source:        source,
		emitter:       emitter,
		metricsTicker: time.NewTicker(interval),
	}

	if sessionStore != nil {
		pipeline.sessionStore = sessionStore
		pipeline.sessionTicker = time.NewTicker(sessionInterval)
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
	go pipeline.startMetricsGatherer()

	if sessionStore != nil {
		pipeline.initState()
		go pipeline.startStateSaver()
	}
	pipeline.emitter.Start()

	return pipeline, nil
}

func (pipeline *Pipeline) String() string {
	out := pipeline.source.String()
	return out
}

// Stop sends a stop signal to the emitter and all the nodes, whether they are running or not.
// the node's database adaptors are expected to clean up after themselves, and stop will block until
// all nodes have stopped successfully
func (pipeline *Pipeline) Stop() {
	pipeline.source.Stop()
	pipeline.emitter.Stop()
	if pipeline.sessionStore != nil {
		pipeline.sessionTicker.Stop()
	}
	pipeline.metricsTicker.Stop()
}

// Run the pipeline
func (pipeline *Pipeline) Run() error {
	endpoints := pipeline.source.Endpoints()
	// send a boot event
	pipeline.source.pipe.Event <- events.NewBootEvent(time.Now().Unix(), VERSION, endpoints)

	// start the source
	err := pipeline.source.Start()
	if err != nil && pipeline.Err == nil {
		pipeline.Err = err // only set it if it hasn't been set already.
	}

	// pipeline has stopped, emit one last round of metrics and send the exit event
	pipeline.emitMetrics()
	if pipeline.sessionStore != nil {
		pipeline.setState()
	}
	pipeline.source.pipe.Event <- events.NewExitEvent(time.Now().Unix(), VERSION, endpoints)

	// the source has exited, stop all the other nodes
	pipeline.Stop()

	return pipeline.Err
}

// start error listener consumes all the events on the pipe's Err channel, and stops the pipeline
// when it receives one
func (pipeline *Pipeline) startErrorListener(cherr chan error) {
	for err := range cherr {
		if aerr, ok := err.(adaptor.Error); ok {
			pipeline.source.pipe.Event <- events.NewErrorEvent(time.Now().Unix(), aerr.Path, aerr.Record, aerr.Error())
			if aerr.Lvl == adaptor.ERROR || aerr.Lvl == adaptor.CRITICAL {
				log.Println(aerr)
			}
		} else {
			if pipeline.Err == nil {
				pipeline.Err = err
			}
			pipeline.Stop()
		}
	}
}

func (pipeline *Pipeline) startMetricsGatherer() {
	for _ = range pipeline.metricsTicker.C {
		pipeline.emitMetrics()
	}
}

// emit the metrics
func (pipeline *Pipeline) emitMetrics() {

	frontier := make([]*Node, 1)
	frontier[0] = pipeline.source

	for {
		// pop the first item
		node := frontier[0]
		frontier = frontier[1:]

		// do something with the node
		pipeline.source.pipe.Event <- events.NewMetricsEvent(time.Now().Unix(), node.Path(), node.pipe.MessageCount)

		// add this nodes children to the frontier
		for _, child := range node.Children {
			frontier = append(frontier, child)
		}

		// if we're empty
		if len(frontier) == 0 {
			break
		}
	}
}

func (pipeline *Pipeline) startStateSaver() {
	for _ = range pipeline.sessionTicker.C {
		pipeline.setState()
	}
}

func (pipeline *Pipeline) setState() {
	frontier := make([]*Node, 1)
	frontier[0] = pipeline.source

	for {
		// pop the first item
		node := frontier[0]
		frontier = frontier[1:]

		// do something with the node
		if node.Type != "transformer" && node.pipe.LastMsg != nil {
			pipeline.sessionStore.Set(node.Path(), &state.MsgState{Msg: node.pipe.LastMsg, Extra: node.pipe.ExtraState})
		}

		// add this nodes children to the frontier
		for _, child := range node.Children {
			frontier = append(frontier, child)
		}

		// if we're empty
		if len(frontier) == 0 {
			break
		}
	}
}

func (pipeline *Pipeline) initState() {
	frontier := make([]*Node, 1)
	frontier[0] = pipeline.source

	for {
		// pop the first item
		node := frontier[0]
		frontier = frontier[1:]

		// do something with the node
		if node.Type != "transformer" {
			nodeState, _ := pipeline.sessionStore.Get(node.Path())
			if nodeState != nil {
				node.pipe.LastMsg = nodeState.Msg
				node.pipe.ExtraState = nodeState.Extra
			}
		}

		// add this nodes children to the frontier
		for _, child := range node.Children {
			frontier = append(frontier, child)
		}

		// if we're empty
		if len(frontier) == 0 {
			break
		}
	}
}
