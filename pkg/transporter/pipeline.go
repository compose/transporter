package transporter

/*
 * A Pipeline is a the end to end description of a transporter data flow.
 * including the source, sink, and all the transformers along the way
 */

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/compose/transporter/pkg/pipe"
)

const (
	VERSION = "0.0.1"
)

type Pipeline struct {
	api Api

	source *Node

	metricsWg *sync.WaitGroup
}

// NewPipeline creates a new Transporter Pipeline, with the given node acting as the Source.
// subsequent nodes should be added via AddNode
func NewPipeline(source *Node, api Api) (*Pipeline, error) {
	pipeline := &Pipeline{
		api:       api,
		metricsWg: &sync.WaitGroup{},
	}

	err := source.Init(api)
	if err != nil {
		return pipeline, err
	}

	pipeline.source = source

	go pipeline.startErrorListener(source.pipe.Err)
	go pipeline.startEventListener(source.pipe.Event)

	return pipeline, nil
}

func (pipeline *Pipeline) String() string {

	out := pipeline.source.String()
	return out
}

// Stop sends a stop signal to all the nodes, whether they are running or not
func (pipeline *Pipeline) Stop() {
	pipeline.source.Stop()
}

// run the pipeline
func (pipeline *Pipeline) Run() error {
	endpoints := pipeline.source.Endpoints()

	// send a boot event
	pipeline.source.pipe.Event <- pipe.NewBootEvent(time.Now().Unix(), VERSION, endpoints)

	// start the source
	err := pipeline.source.Start()

	// the source has exited, stop all the other nodes
	pipeline.Stop()

	// pipeline.nodeWg.Wait()
	pipeline.metricsWg.Wait()

	// send a boot event
	pipeline.source.pipe.Event <- pipe.NewExitEvent(time.Now().Unix(), VERSION, endpoints)

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

// startEventListener consumes all the events from the pipe's Event channel, and posts them to the ap
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
			if pipeline.api.Uri != "" {
				req, err := http.NewRequest("POST", pipeline.api.Uri, bytes.NewBuffer(ba))
				req.Header.Set("Content-Type", "application/json")
				if len(pipeline.api.Pid) > 0 && len(pipeline.api.Key) > 0 {
					req.SetBasicAuth(pipeline.api.Pid, pipeline.api.Key)
				}
				cli := &http.Client{}
				resp, err := cli.Do(req)
				if err != nil {
					fmt.Println("event send failed")
					pipeline.source.pipe.Err <- err
					return
				}

				defer resp.Body.Close()
				body, err := ioutil.ReadAll(resp.Body)

				if resp.StatusCode != 200 && resp.StatusCode != 201 {
					pipeline.source.pipe.Err <- fmt.Errorf("Event Error: http error code, expected 200 or 201, got %d.  %d\n\t%s", resp.StatusCode, resp.StatusCode, body)
					return
				}
				resp.Body.Close()
			}
		}()
		if pipeline.api.Uri != "" {
			fmt.Printf("sent pipeline event: %s -> %s\n", pipeline.api.Uri, event)
		}

	}
}
