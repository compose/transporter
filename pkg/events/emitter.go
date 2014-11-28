package events

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
)

type Emitter interface {
	Start()
	Init(chan Event)
	Stop()
}

// An Api is the definition of the remote endpoint that receieves event and error posts
// for the HttpPostEmitter
type Api struct {
	Uri             string `json:"uri" yaml:"uri"`
	MetricsInterval int    `json:"interval" yaml:"interval"`
	Key             string `json:"key" yaml:"key"`
	Pid             string `json:"pid" yaml:"pid"`
}

// HttpPostEmitter listens on the event channel and posts the events to an http server
type HttpPostEmitter struct {
	api Api

	inflight *sync.WaitGroup
	ch       chan Event
	chstop   chan chan bool
}

func NewHttpPostEmitter(api Api) *HttpPostEmitter {
	return &HttpPostEmitter{
		api:      api,
		chstop:   make(chan chan bool),
		inflight: &sync.WaitGroup{},
	}
}

// Start the emitter
func (e *HttpPostEmitter) Start() {
	go e.startEventListener()
}

func (e *HttpPostEmitter) Init(ch chan Event) {
	e.ch = ch
}

// Stop the emitter
func (e *HttpPostEmitter) Stop() {
	s := make(chan bool)
	e.chstop <- s
	<-s
}

func (e *HttpPostEmitter) startEventListener() {
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

// NoopEmitter consumes the events from the listening channel and does nothing with them
type NoopEmitter struct {
	chstop chan chan bool
	ch     chan Event
}

func NewNoopEmitter() *NoopEmitter {
	return &NoopEmitter{chstop: make(chan chan bool)}
}

// consume evennts
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

func (e *NoopEmitter) Init(ch chan Event) {
	e.ch = ch
}

func (e *NoopEmitter) Stop() {
	s := make(chan bool)
	e.chstop <- s
	<-s
}
