package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
)

// Emitters are used by the transporter pipeline to consume events from a pipeline's event channel
// and process them.
// Start() will start the emitter and being consuming events
// Init() serves to set the Emitter's listening channel
// Stop() stops the event loop and releases any resources.  Stop is expected to shut down the process cleanly,
// the pipeline process will block until Stop() returns
type Emitter interface {
	Start()
	Init(chan Event)
	Stop()
}

// An Api is the definition of the remote endpoint that receieves event and error posts
// for the HttpPostEmitter.
// TODO it's kind of janky that this is here, it would be great if this didn't exist, and the
// HttpPostEmitter just took the uri, etc etc as args.  MetricsInterval isn't even relevent to this
// package
type Api struct {
	Uri             string `json:"uri" yaml:"uri"`           // Uri to connect to
	MetricsInterval int    `json:"interval" yaml:"interval"` // how often to emit metrics, (in ms)
	Key             string `json:"key" yaml:"key"`           // http basic auth password to send with each event
	Pid             string `json:"pid" yaml:"pid"`           // http basic auth username to send with each event
}

// HttpPostEmitter listens on the event channel and posts the events to an http server
// Events are serialized into json, and sent via a POST request to the given Uri
// http errors are logged as warnings to the console, and won't stop the Emitter
func HttpPostEmitter(api Api) *httpPostEmitter {
	return &httpPostEmitter{
		api:      api,
		chstop:   make(chan chan bool),
		inflight: &sync.WaitGroup{},
	}
}

type httpPostEmitter struct {
	api Api

	inflight *sync.WaitGroup
	ch       chan Event
	chstop   chan chan bool
}

// Start the emitter
func (e *httpPostEmitter) Start() {
	go e.startEventListener()
}

// Init Set's the event channel
func (e *httpPostEmitter) Init(ch chan Event) {
	e.ch = ch
}

// Stop sends a stop signal and waits for the inflight posts to complete before exiting
func (e *httpPostEmitter) Stop() {
	s := make(chan bool)
	e.chstop <- s
	<-s
}

func (e *httpPostEmitter) startEventListener() {
	for {
		select {
		case s := <-e.chstop:
			s <- true
			return
		case event := <-e.ch:
			e.inflight.Add(1)
			go func(event Event) {
				defer e.inflight.Done()
				ba, err := json.Marshal(event)
				if err != err {
					log.Printf("EventEmitter Error: %s", err)
					return
				}

				req, err := http.NewRequest("POST", e.api.Uri, bytes.NewBuffer(ba))
				req.Header.Set("Content-Type", "application/json")
				if len(e.api.Pid) > 0 && len(e.api.Key) > 0 {
					req.SetBasicAuth(e.api.Pid, e.api.Key)
				}
				cli := &http.Client{}
				resp, err := cli.Do(req)
				defer resp.Body.Close()
				if err != nil {
					log.Printf("EventEmitter Error: %s", err)
					return
				}
				_, err = ioutil.ReadAll(resp.Body)
				if resp.StatusCode != 200 && resp.StatusCode != 201 {
					log.Printf("EventEmitter Error: http error code, expected 200 or 201, got %d", resp.StatusCode)
					return
				}
			}(event)
		case <-time.After(100 * time.Millisecond):
			continue
			// noop
		}
	}
}

// NoopEmitter constructs a NoopEmitter to use with a transporter pipeline.
// a NoopEmitter consumes the events from the listening channel and does nothing with them
// this is useful for cli utilities that dump output to stdout in any case, and don't want
// to clutter the program's output with metrics
func NoopEmitter() *noopEmitter {
	return &noopEmitter{chstop: make(chan chan bool)}
}

// NoopEmitter consumes the events from the listening channel and does nothing with them
// this is useful for cli utilities that dump output to stdout in any case, and don't want
// to clutter the program's output with metrics
type noopEmitter struct {
	chstop chan chan bool
	ch     chan Event
}

// consume events
func (e *noopEmitter) Start() {
	go func() {
		for {
			select {
			case s := <-e.chstop:
				s <- true
				return
			case <-e.ch:
				continue
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
	}()
}

// Init Set's the event channel
func (e *noopEmitter) Init(ch chan Event) {
	e.ch = ch
}

func (e *noopEmitter) Stop() {
	s := make(chan bool)
	e.chstop <- s
	<-s
}

// LogEmitter constructs a LogEmitter to use with a transporter pipeline.
// A LogEmitter listens on the event channel and uses go's log package to emit the event,
// eg.
//   2014/11/28 16:56:58 boot map[source:mongo out:mongo]
//   2014/11/28 16:56:58 metrics source recordsIn: 0, recordsOut: 203
//   2014/11/28 16:56:58 exit
//   2014/11/28 16:56:58 metrics source/out recordsIn: 203, recordsOut: 0
func LogEmitter() *logEmitter {
	return &logEmitter{
		chstop: make(chan chan bool),
	}
}

// LogEmitter listens on the event channel and uses go's log package to emit the event,
type logEmitter struct {
	chstop chan chan bool
	ch     chan Event
}

// Start the emitter
func (e *logEmitter) Start() {
	go e.startEventListener()
}

// Init Set's the event channel
func (e *logEmitter) Init(ch chan Event) {
	e.ch = ch
}

// Stop the emitter
func (e *logEmitter) Stop() {
	s := make(chan bool)
	e.chstop <- s
	<-s
}

func (e *logEmitter) startEventListener() {
	for {
		select {
		case s := <-e.chstop:
			s <- true
			return
		case event := <-e.ch:
			msg := fmt.Sprintf("%s %s", event.Kind, event.Path)

			switch event.Kind {
			case metricsKind.String():
				msg += fmt.Sprintf(" recordsIn: %d, recordsOut: %d", event.RecordsIn, event.RecordsOut)
			case bootKind.String():
				msg += fmt.Sprintf("%v", event.Endpoints)

			}
			log.Println(msg)
		case <-time.After(100 * time.Millisecond):
			continue
			// noop
		}
	}
}
