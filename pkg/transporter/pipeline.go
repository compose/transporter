package transporter

import (
	"time"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/events"
	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/state"
)

// A Pipeline is a the end to end description of a transporter data flow.
// including the source, sink, and all the transformers along the way
type Pipeline struct {
	Source        *Node
	emitter       events.Emitter
	sessionStore  state.SessionStore
	metricsTicker *time.Ticker
	version       string

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
func NewDefaultPipeline(source *Node, uri, key, pid, version string, interval time.Duration) (*Pipeline, error) {
	return NewPipeline(version, source, events.HTTPPostEmitter(uri, key, pid), interval, nil, 10*time.Second)
}

// NewPipeline creates a new Transporter Pipeline using the given tree of nodes, and Event Emitter
// eg.
//   source :=
//   	transporter.NewNode("source", "mongo", adaptor.Config{"uri": "mongodb://localhost/", "namespace": "boom.foo", "debug": false, "tail": true}).
// 	  	Add(transporter.NewNode("out", "file", adaptor.Config{"uri": "stdout://"}))
//   pipeline, err := transporter.NewPipeline("version", source, events.NewNoopEmitter(), 1*time.Second, state.NewFilestore(pid, "/tmp/transporter.state"), 10*time.Second)
//   if err != nil {
// 	  fmt.Println(err)
// 	  os.Exit(1)
//   }
// pipeline.Run()
func NewPipeline(version string, source *Node, emit events.EmitFunc, interval time.Duration, sessionStore state.SessionStore, sessionInterval time.Duration) (*Pipeline, error) {

	pipeline := &Pipeline{
		Source:        source,
		metricsTicker: time.NewTicker(interval),
	}

	if sessionStore != nil {
		pipeline.sessionStore = sessionStore
		pipeline.sessionTicker = time.NewTicker(sessionInterval)
	}

	// init the pipeline
	err := pipeline.Source.Init(interval)
	if err != nil {
		return pipeline, err
	}

	// init the emitter with the right chan
	pipeline.emitter = events.NewEmitter(source.pipe.Event, emit)

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
	return pipeline.Source.String()
}

// Stop sends a stop signal to the emitter and all the nodes, whether they are running or not.
// the node's database adaptors are expected to clean up after themselves, and stop will block until
// all nodes have stopped successfully
func (pipeline *Pipeline) Stop() {
	endpoints := pipeline.Source.Endpoints()
	pipeline.Source.Stop()
	if pipeline.sessionStore != nil {
		pipeline.sessionTicker.Stop()
	}

	// pipeline has stopped, emit one last round of metrics and send the exit event
	pipeline.emitMetrics()
	pipeline.Source.pipe.Event <- events.NewExitEvent(time.Now().UnixNano(), pipeline.version, endpoints)
	pipeline.emitter.Stop()

	pipeline.metricsTicker.Stop()
}

// Run the pipeline
func (pipeline *Pipeline) Run() error {
	endpoints := pipeline.Source.Endpoints()
	// send a boot event
	pipeline.Source.pipe.Event <- events.NewBootEvent(time.Now().UnixNano(), pipeline.version, endpoints)

	if pipeline.sessionStore != nil {
		pipeline.setState()
	}

	// start the source
	err := pipeline.Source.Start()
	if err != nil && pipeline.Err == nil {
		pipeline.Err = err // only set it if it hasn't been set already.
	}

	return pipeline.Err
}

// start error listener consumes all the events on the pipe's Err channel, and stops the pipeline
// when it receives one
func (pipeline *Pipeline) startErrorListener(cherr chan error) {
	for err := range cherr {
		if aerr, ok := err.(adaptor.Error); ok {
			pipeline.Source.pipe.Event <- events.NewErrorEvent(time.Now().UnixNano(), aerr.Path, aerr.Record, aerr.Error())
			if aerr.Lvl == adaptor.ERROR || aerr.Lvl == adaptor.CRITICAL {
				log.With("path", aerr.Path).Errorln(aerr)
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
	for range pipeline.metricsTicker.C {
		pipeline.emitMetrics()
	}
}

// emit the metrics
func (pipeline *Pipeline) emitMetrics() {
	pipeline.apply(func(node *Node) {
		pipeline.Source.pipe.Event <- events.NewMetricsEvent(time.Now().UnixNano(), node.Path(), node.pipe.MessageCount)
	})
}

func (pipeline *Pipeline) startStateSaver() {
	for range pipeline.sessionTicker.C {
		pipeline.setState()
	}
}

func (pipeline *Pipeline) setState() {
	pipeline.apply(func(node *Node) {
		if node.Type != "transformer" && node.pipe.LastMsg != nil {
			pipeline.sessionStore.Set(node.Path(), &state.MsgState{Msg: node.pipe.LastMsg, Extra: node.pipe.ExtraState})
		}
	})
}

func (pipeline *Pipeline) initState() {
	pipeline.apply(func(node *Node) {
		if node.Type != "transformer" {
			state, _ := pipeline.sessionStore.Get(node.Path())
			if state != nil {
				node.pipe.LastMsg = state.Msg
				node.pipe.ExtraState = state.Extra
			}
		}
	})
}

// apply maps a function f across all nodes of a pipeline
func (pipeline *Pipeline) apply(f func(*Node)) {
	if pipeline.Source == nil {
		return
	}
	head := pipeline.Source
	nodes := []*Node{head}
	for len(nodes) > 0 {
		head, nodes = nodes[0], nodes[1:]
		f(head)
		nodes = append(nodes, head.Children...)
	}
}
