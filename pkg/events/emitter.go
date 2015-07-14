package events

import (
	"bytes"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
)

// Emitter types are used by the transporter pipeline to consume events from a pipeline's event channel
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

// HTTPPostEmitter listens on the event channel and posts the events to an http server
// Events are serialized into json, and sent via a POST request to the given Uri
// http errors are logged as warnings to the console, and won't stop the Emitter
type HTTPPostEmitter struct {
	uri string
	key string
	pid string

	inflight *sync.WaitGroup
	ch       chan Event
	chstop   chan chan bool
}

// NewHTTPPostEmitter creates a new HTTPPostEmitter
func NewHTTPPostEmitter(uri, key, pid string) *HTTPPostEmitter {
	return &HTTPPostEmitter{
		uri:      uri,
		key:      key,
		pid:      pid,
		chstop:   make(chan chan bool),
		inflight: &sync.WaitGroup{},
	}
}

// Start the emitter
func (e *HTTPPostEmitter) Start() {
	go e.startEventListener()
}

// Init sets the event channel
func (e *HTTPPostEmitter) Init(ch chan Event) {
	e.ch = ch
}

// Stop sends a stop signal and waits for the inflight posts to complete before exiting
func (e *HTTPPostEmitter) Stop() {
	s := make(chan bool)
	e.chstop <- s
	<-s
	e.inflight.Wait()
}

func (e *HTTPPostEmitter) startEventListener() {
	for {
		select {
		case s := <-e.chstop:
			s <- true
			return
		case event := <-e.ch:
			e.inflight.Add(1)
			go func(event Event) {
				defer e.inflight.Done()

				ba, err := event.Emit()
				if err != err {
					log.Printf("EventEmitter Error: %s", err)
					return
				}

				req, err := http.NewRequest("POST", e.uri, bytes.NewBuffer(ba))
				if err != nil {
					log.Printf("EventEmitter Error: %s", err)
					return
				}
				req.Header.Set("Content-Type", "application/json")
				if len(e.pid) > 0 && len(e.key) > 0 {
					req.SetBasicAuth(e.pid, e.key)
				}
				cli := &http.Client{}
				resp, err := cli.Do(req)

				if err != nil {
					log.Printf("EventEmitter Error: %s", err)
					return
				}
				_, err = ioutil.ReadAll(resp.Body)
				defer resp.Body.Close()
				if resp.StatusCode != 200 && resp.StatusCode != 201 {
					log.Printf("EventEmitter Error: http error code, expected 200 or 201, got %d, (%s)", resp.StatusCode, ba)
					return
				}
				// fmt.Printf("EventEmitter, got http statuscode:%d for event: %s", resp.StatusCode, event)
			}(event)
		case <-time.After(100 * time.Millisecond):
			continue
			// noop
		}
	}
}

// NewNoopEmitter constructs a NoopEmitter to use with a transporter pipeline.
// a NoopEmitter consumes the events from the listening channel and does nothing with them
// this is useful for cli utilities that dump output to stdout in any case, and don't want
// to clutter the program's output with metrics
func NewNoopEmitter() *NoopEmitter {
	return &NoopEmitter{chstop: make(chan chan bool)}
}

// NoopEmitter consumes the events from the listening channel and does nothing with them
// this is useful for cli utilities that dump output to stdout in any case, and don't want
// to clutter the program's output with metrics
type NoopEmitter struct {
	chstop chan chan bool
	ch     chan Event
}

// Start the event consumer
func (e *NoopEmitter) Start() {
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

// Init sets the event channel
func (e *NoopEmitter) Init(ch chan Event) {
	e.ch = ch
}

// Stop the event consumer
func (e *NoopEmitter) Stop() {
	s := make(chan bool)
	e.chstop <- s
	<-s
}

// NewLogEmitter creates a new LogEmitter
func NewLogEmitter() *LogEmitter {
	return &LogEmitter{
		chstop: make(chan chan bool),
	}
}

// LogEmitter constructs a LogEmitter to use with a transporter pipeline.
// A LogEmitter listens on the event channel and uses go's log package to emit the event,
// eg.
//   2014/11/28 16:56:58 boot map[source:mongo out:mongo]
//   2014/11/28 16:56:58 metrics source recordsIn: 0, recordsOut: 203
//   2014/11/28 16:56:58 exit
//   2014/11/28 16:56:58 metrics source/out recordsIn: 203, recordsOut: 0
type LogEmitter struct {
	chstop chan chan bool
	ch     chan Event
}

// Start the emitter
func (e *LogEmitter) Start() {
	go e.startEventListener()
}

// Init sets the event channel
func (e *LogEmitter) Init(ch chan Event) {
	e.ch = ch
}

// Stop the emitter
func (e *LogEmitter) Stop() {
	s := make(chan bool)
	e.chstop <- s
	<-s
}

func (e *LogEmitter) startEventListener() {
	for {
		select {
		case s := <-e.chstop:
			s <- true
			return
		case event := <-e.ch:
			log.Println(event.String())
		case <-time.After(100 * time.Millisecond):
			continue
			// noop
		}
	}
}

// NewJsonLogEmitter creates a new LogEmitter
func NewJsonLogEmitter() *JsonLogEmitter {
	return &JsonLogEmitter{
		chstop: make(chan chan bool),
	}
}

// JsonLogEmitter constructs a LogEmitter to use with a transporter pipeline.
// A JsonLogEmitter listens on the event channel and uses go's log package to emit the event,
// eg.
// 2015/07/14 11:52:01 {"ts":1436889121,"name":"metrics","path":"source-development.jobs/dest-x.jobs","records":121}
// 2015/07/14 11:52:01 {"ts":1436889121,"name":"exit","version":"0.0.4","endpoints":{"dest-x.jobs":"mongo","source-development.jobs":"mongo"}}
type JsonLogEmitter struct {
	chstop chan chan bool
	ch     chan Event
}

// Start the emitter
func (e *JsonLogEmitter) Start() {
	go e.startEventListener()
}

// Init sets the event channel
func (e *JsonLogEmitter) Init(ch chan Event) {
	e.ch = ch
}

// Stop the emitter
func (e *JsonLogEmitter) Stop() {
	s := make(chan bool)
	e.chstop <- s
	<-s
}

func (e *JsonLogEmitter) startEventListener() {
	for {
		select {
		case s := <-e.chstop:
			s <- true
			return
		case event := <-e.ch:
			j, err := event.Emit()
			if err != nil {
				continue
			}
			log.Println(string(j))
		case <-time.After(100 * time.Millisecond):
			continue
			// noop
		}
	}
}
