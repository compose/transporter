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
	Ts        int64             `json:"ts"`
	Kind      string            `json:"name"`
	Version   string            `json:"version,omitempty"`
	Endpoints map[string]string `json:"endpoints,omitempty"`
}

// BootEvent (surprisingly) creates a new baseEvent
func BootEvent(ts int64, version string, endpoints map[string]string) *baseEvent {
	e := &baseEvent{
		Ts:        ts,
		Kind:      "boot",
		Version:   version,
		Endpoints: endpoints,
	}
	return e
}

// ExitEvent (surprisingly) creates a new baseEvent
func ExitEvent(ts int64, version string, endpoints map[string]string) *baseEvent {
	e := &baseEvent{
		Ts:        ts,
		Kind:      "exit",
		Version:   version,
		Endpoints: endpoints,
	}
	return e
}

func (e *baseEvent) Emit() ([]byte, error) {
	return json.Marshal(e)
}

func (e *baseEvent) String() string {
	msg := fmt.Sprintf("%s", e.Kind)
	msg += fmt.Sprintf("%v", e.Endpoints)
	return msg
}

type metricsEvent struct {
	Ts         int64  `json:"ts"`
	Kind       string `json:"name"`
	Path       string `json:"path,omitempty"`
	RecordsIn  int    `json:"records_in,omitempty"`
	RecordsOut int    `json:"records_out,omitempty"`
}

// MetricsEvent creates a new metrics event
func MetricsEvent(ts int64, path string, in, out int) *metricsEvent {
	e := &metricsEvent{
		Ts:         ts,
		Kind:       "metrics",
		Path:       path,
		RecordsIn:  in,
		RecordsOut: out,
	}
	return e
}

func (e *metricsEvent) Emit() ([]byte, error) {
	return json.Marshal(e)
}

func (e *metricsEvent) String() string {
	msg := fmt.Sprintf("%s %s", e.Kind, e.Path)
	msg += fmt.Sprintf(" recordsIn: %d, recordsOut: %d", e.RecordsIn, e.RecordsOut)
	return msg
}

type errorEvent struct {
	Ts      int64  `json:"ts"`
	Kind    string `json:"name"`
	Path    string `json:"path"`
	Record  bson.M `json:"record,omitempty"`
	Message string `json:"message,omitempty"`
}

// ErrorEvents are sent to indicate a problem processing on one of the nodes
func ErrorEvent(ts int64, path string, record bson.M, message string) *errorEvent {
	e := &errorEvent{
		Ts:      ts,
		Kind:    "error",
		Path:    path,
		Record:  record,
		Message: message,
	}
	return e
}

func (e *errorEvent) Emit() ([]byte, error) {
	return json.Marshal(e)
}

func (e *errorEvent) String() string {
	msg := fmt.Sprintf("%s", e.Kind)
	msg += fmt.Sprintf(" record: %v, message: %s", e.Record, e.Message)
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
