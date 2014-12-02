package events

import (
	"encoding/json"
	"fmt"
	"time"

	"gopkg.in/mgo.v2/bson"
)

// an Event is produced periodically by the running transporter.
//
// Events come in multiple kinds.  baseEvents are emitted when the transporter starts and stops,
// metricsEvents are emittied by each pipe and include a measure of how many messages have been processed
type Event interface {
	Emit() ([]byte, error)
	String() string
}

// baseevents are sent when the pipeline has been started or exited
type baseEvent struct {
	ts        int64             `json:"ts"`
	kind      string            `json:"name"`
	version   string            `json:"version,omitempty"`
	endpoints map[string]string `json:"endpoints,omitempty"`
}

// BootEvent (surprisingly) creates a new baseEvent
func BootEvent(ts int64, version string, endpoints map[string]string) *baseEvent {
	e := &baseEvent{
		ts:        ts,
		kind:      "boot",
		version:   version,
		endpoints: endpoints,
	}
	return e
}

// ExitEvent (surprisingly) creates a new baseEvent
func ExitEvent(ts int64, version string, endpoints map[string]string) *baseEvent {
	e := &baseEvent{
		ts:        ts,
		kind:      "exit",
		version:   version,
		endpoints: endpoints,
	}
	return e
}

func (e *baseEvent) Emit() ([]byte, error) {
	return json.Marshal(e)
}

func (e *baseEvent) String() string {
	msg := fmt.Sprintf("%s", e.kind)
	msg += fmt.Sprintf("%v", e.endpoints)
	return msg
}

type metricsEvent struct {
	ts         int64  `json:"ts"`
	kind       string `json:"name"`
	path       string `json:"path,omitempty"`
	recordsIn  int    `json:"records_in,omitempty"`
	recordsOut int    `json:"records_out,omitempty"`
}

// MetricsEvent creates a new metrics event
func MetricsEvent(ts int64, path string, in, out int) *metricsEvent {
	e := &metricsEvent{
		ts:         ts,
		kind:       "metrics",
		path:       path,
		recordsIn:  in,
		recordsOut: out,
	}
	return e
}

func (e *metricsEvent) Emit() ([]byte, error) {
	return json.Marshal(e)
}

func (e *metricsEvent) String() string {
	msg := fmt.Sprintf("%s %s", e.kind, e.path)
	msg += fmt.Sprintf(" recordsIn: %d, recordsOut: %d", e.recordsIn, e.recordsOut)
	return msg
}

type errorEvent struct {
	ts      int64  `json:"ts"`
	kind    string `json:"name"`
	record  bson.M `json:"record,omitempty"`
	message string `json:"message,omitempty"`
}

// ErrorEvents are sent to indicate a problem processing on one of the nodes
func ErrorEvent(ts int64, record bson.M, message string) *errorEvent {
	e := &errorEvent{
		ts:      ts,
		kind:    "error",
		record:  record,
		message: message,
	}
	return e
}

func (e *errorEvent) Emit() ([]byte, error) {
	return json.Marshal(e)
}

func (e *errorEvent) String() string {
	msg := fmt.Sprintf("%s", e.kind)
	msg += fmt.Sprintf(" record: %v, message: %s", e.record, e.message)
	return msg
}

//
// lets keep track of metrics on a nodeadaptor, and send them out periodically to our event chan
type NodeMetrics struct {
	ticker     *time.Ticker
	eChan      chan Event
	path       string
	RecordsIn  int
	RecordsOut int
}

// NewNodeMetrics creates a struct that will emit metric events periodically
func NewNodeMetrics(path string, eventChan chan Event, interval time.Duration) *NodeMetrics {
	m := &NodeMetrics{path: path, eChan: eventChan}

	// if we have a non zero interval then spawn a ticker to send metrics out the channel
	if interval > 0 {
		m.ticker = time.NewTicker(interval)
		go func() {
			for _ = range m.ticker.C {
				m.eChan <- MetricsEvent(time.Now().Unix(), m.path, m.RecordsIn, m.RecordsOut)
			}
		}()
	}
	return m
}

// Stop stops the ticker that sends out new metrics and broadcast a final metric for the node.
// This shuts down the nodeMetrics.
func (m *NodeMetrics) Stop() {
	if m.ticker != nil {
		m.ticker.Stop()
	}
	m.eChan <- MetricsEvent(time.Now().Unix(), m.path, m.RecordsIn, m.RecordsOut)
}
