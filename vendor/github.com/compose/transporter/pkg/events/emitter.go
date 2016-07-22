package events

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
)

// Emitter types are used by the transporter pipeline to consume events from a pipeline's event channel
// and process them.
// Start() will start the emitter and being consuming events
// Stop() stops the event loop and releases any resources.  Stop is expected to shut down the process cleanly,
// the pipeline process will block until Stop() returns
type Emitter interface {
	Start()
	Stop()
}

// emitter is the implementation of Emitter
type emitter struct {
	listenChan chan Event
	emit       EmitFunc
	stop       chan struct{}
	wg         *sync.WaitGroup
	started    bool
}

// EmitFunc is a function that takes an Event as input and emits it
type EmitFunc func(Event) error

// NewEmitter creates a new emitter that will listen on the listen channel and use the emit EmitFunc
// to process events
func NewEmitter(listen chan Event, emit EmitFunc) Emitter {
	return &emitter{
		listenChan: listen,
		emit:       emit,
		stop:       make(chan struct{}),
		wg:         &sync.WaitGroup{},
		started:    false,
	}
}

// Start the emitter
func (e *emitter) Start() {
	if !e.started {
		e.started = true
		go e.startEventListener()
	}
}

// Stop sends a stop signal and waits for the inflight posts to complete before exiting
func (e *emitter) Stop() {
	e.stop <- struct{}{}
	e.wg.Wait()
	e.started = false
}

func (e *emitter) startEventListener() {
	for {
		select {
		case <-e.stop:
			return
		case event := <-e.listenChan:
			e.wg.Add(1)
			go func(event Event) {
				defer e.wg.Done()
				err := e.emit(event)
				if err != nil {
					log.Print(err)
				}
			}(event)
		case <-time.After(100 * time.Millisecond):
			continue
			// noop
		}
	}
}

// HTTPPostEmitter listens on the event channel and posts the events to an http server
// Events are serialized into json, and sent via a POST request to the given Uri
// http errors are logged as warnings to the console, and won't stop the Emitter
func HTTPPostEmitter(uri, key, pid string) EmitFunc {
	return EmitFunc(func(event Event) error {
		ba, err := event.Emit()
		if err != err {
			return err
		}

		req, err := http.NewRequest("POST", uri, bytes.NewBuffer(ba))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		if len(pid) > 0 && len(key) > 0 {
			req.SetBasicAuth(pid, key)
		}
		cli := &http.Client{}
		resp, err := cli.Do(req)

		if err != nil {
			return err
		}
		_, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 && resp.StatusCode != 201 {
			return fmt.Errorf("http error code, expected 200 or 201, got %d, (%s)", resp.StatusCode, ba)
		}
		return nil
	})
}

// NoopEmitter consumes the events from the listening channel and does nothing with them
// this is useful for cli utilities that dump output to stdout in any case, and don't want
// to clutter the program's output with metrics
func NoopEmitter() EmitFunc {
	return EmitFunc(func(event Event) error { return nil })
}

// LogEmitter constructs a LogEmitter to use with a transporter pipeline.
// A LogEmitter listens on the event channel and uses go's log package to emit the event,
// eg.
//   2014/11/28 16:56:58 boot map[source:mongo out:mongo]
//   2014/11/28 16:56:58 metrics source recordsIn: 0, recordsOut: 203
//   2014/11/28 16:56:58 exit
//   2014/11/28 16:56:58 metrics source/out recordsIn: 203, recordsOut: 0
func LogEmitter() EmitFunc {
	return EmitFunc(func(event Event) error {
		log.Println(event.String())
		return nil
	})
}

// JSONLogEmitter constructs a LogEmitter to use with a transporter pipeline.
// A JsonLogEmitter listens on the event channel and uses go's log package to emit the event,
// eg.
// 2015/07/14 11:52:01 {"ts":1436889121,"name":"metrics","path":"source-development.jobs/dest-x.jobs","records":121}
// 2015/07/14 11:52:01 {"ts":1436889121,"name":"exit","version":"0.0.4","endpoints":{"dest-x.jobs":"mongo","source-development.jobs":"mongo"}}
func JSONLogEmitter() EmitFunc {
	return EmitFunc(func(event Event) error {
		j, err := event.Emit()
		if err != nil {
			return err
		}
		log.Println(string(j))
		return nil
	})
}
