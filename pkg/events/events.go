package events

import (
	"encoding/json"
	"fmt"
)

// Event is an interface that describes data which is produced periodically by the running transporter.
//
// Events come in multiple kinds. BaseEvents are emitted when the transporter starts and stops,
// metricsEvents are emitted by each pipe and include a measure of how many messages have been processed
type Event interface {
	Emit() ([]byte, error)
	String() string
}

// BaseEvent is an event that is sent when the pipeline has been started or exited
type baseEvent struct {
	Ts        int64             `json:"ts"`
	Kind      string            `json:"name"`
	Version   string            `json:"version,omitempty"`
	Endpoints map[string]string `json:"endpoints,omitempty"`
}

// NewBootEvent (surprisingly) creates a new BaseEvent
func NewBootEvent(ts int64, version string, endpoints map[string]string) Event {
	e := &baseEvent{
		Ts:        ts,
		Kind:      "boot",
		Version:   version,
		Endpoints: endpoints,
	}
	return e
}

// NewExitEvent (surprisingly) creates a new BaseEvent
func NewExitEvent(ts int64, version string, endpoints map[string]string) Event {
	e := &baseEvent{
		Ts:        ts,
		Kind:      "exit",
		Version:   version,
		Endpoints: endpoints,
	}
	return e
}

// Emit prepares the event to be emitted and marshals the event into a JSON
func (e *baseEvent) Emit() ([]byte, error) {
	return json.Marshal(e)
}

// String
func (e *baseEvent) String() string {
	return fmt.Sprintf("%s %v", e.Kind, e.Endpoints)
}

// metricsEvent is an event used to indicated progress.
type metricsEvent struct {
	Ts   int64  `json:"ts"`
	Kind string `json:"name"`
	Path string `json:"path"`

	// Records indicates the total number of documents that have been transmitted
	Records int `json:"records"`
}

// NewMetricsEvent creates a new metrics event
func NewMetricsEvent(ts int64, path string, records int) Event {
	e := &metricsEvent{
		Ts:      ts,
		Kind:    "metrics",
		Path:    path,
		Records: records,
	}
	return e
}

// Emit prepares the event to be emitted and marshalls the event into an json
func (e *metricsEvent) Emit() ([]byte, error) {
	return json.Marshal(e)
}

func (e *metricsEvent) String() string {
	return fmt.Sprintf("%s %s records: %d", e.Kind, e.Path, e.Records)
}

// errorEvent is an event that indicates an error occurred
// during the processing of a pipeline
type errorEvent struct {
	Ts   int64  `json:"ts"`
	Kind string `json:"name"`
	Path string `json:"path"`

	// Record is the document (if any) that was in progress when the error occurred
	Record interface{} `json:"record,omitempty"`

	// Message is the error message as a string
	Message string `json:"message,omitempty"`
}

// NewErrorEvent are events sent to indicate a problem processing on one of the nodes
func NewErrorEvent(ts int64, path string, record interface{}, message string) Event {
	e := &errorEvent{
		Ts:      ts,
		Kind:    "error",
		Path:    path,
		Record:  record,
		Message: message,
	}
	return e
}

// Emit prepares the event to be emitted and marshalls the event into an json
func (e *errorEvent) Emit() ([]byte, error) {
	return json.Marshal(e)
}

// String
func (e *errorEvent) String() string {
	msg := fmt.Sprintf("%s", e.Kind)
	msg += fmt.Sprintf(" record: %v, message: %s", e.Record, e.Message)
	return msg
}
