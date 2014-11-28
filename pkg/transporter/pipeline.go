package transporter

/*
 * A Pipeline is a the end to end description of a transporter data flow.
 * including the source, sink, and all the transformers along the way
 */

import (
	// "bytes"
	// "encoding/json"
	"fmt"
	// "io/ioutil"
	// "net/http"
	// "sync"
	"time"

	"github.com/compose/transporter/pkg/events"
)

const (
	VERSION = "0.0.1"
)

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

// // startEventListener consumes all the events from the pipe's Event channel, and posts them to the ap
// func (pipeline *Pipeline) startEventListener(chevent chan events.Event) {
// 	for event := range chevent {
// 		ba, err := json.Marshal(event)
// 		if err != err {
// 			pipeline.source.pipe.Err <- err
// 			continue
// 		}
// 		pipeline.metricsWg.Add(1)
// 		go func() {
// 			defer pipeline.metricsWg.Done()
// 			if pipeline.api.Uri != "" {
// 				req, err := http.NewRequest("POST", pipeline.api.Uri, bytes.NewBuffer(ba))
// 				req.Header.Set("Content-Type", "application/json")
// 				if len(pipeline.api.Pid) > 0 && len(pipeline.api.Key) > 0 {
// 					req.SetBasicAuth(pipeline.api.Pid, pipeline.api.Key)
// 				}
// 				cli := &http.Client{}
// 				resp, err := cli.Do(req)
// 				if err != nil {
// 					fmt.Println("event send failed")
// 					pipeline.source.pipe.Err <- err
// 					return
// 				}

// 				defer resp.Body.Close()
// 				body, err := ioutil.ReadAll(resp.Body)

// 				if resp.StatusCode != 200 && resp.StatusCode != 201 {
// 					pipeline.source.pipe.Err <- fmt.Errorf("Event Error: http error code, expected 200 or 201, got %d.  %d\n\t%s", resp.StatusCode, resp.StatusCode, body)
// 					return
// 				}
// 				resp.Body.Close()
// 			}
// 		}()
// 		if pipeline.api.Uri != "" {
// 			fmt.Printf("sent pipeline event: %s -> %s\n", pipeline.api.Uri, event)
// 		}
// 	}
// }
