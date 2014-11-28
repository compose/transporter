package events

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

type Emitter interface {
	Start()
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

type HttpPostEmitter struct {
	api Api

	inflight *sync.WaitGroup
	ch       chan Event
	chstop   chan chan bool
}

func NewHttpPostEmitter(api Api, ch chan Event) *HttpPostEmitter {
	return &HttpPostEmitter{api: api, ch: ch, chstop: make(chan chan bool)}
}

func (e *HttpPostEmitter) Start() {
	go e.startEventListener()
}

func (e *HttpPostEmitter) Stop() {
	s := make(chan bool)
	e.chstop <- s
	<-s
}

func (e *HttpPostEmitter) startEventListener() {
	for event := range e.ch {
		select {
		case s := <-e.chstop:
			s <- true
		default:
			// noop
		}

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
	}
}
