package events

import (
	"encoding/json"
	"fmt"
)

// Event is an interface that describes data which is produced periodically by the running transporter.
//
// Events come in multiple kinds.  baseEvents are emitted when the transporter starts and stops,
// metricsEvents are emittied by each pipe and include a measure of how many messages have been processed
type Event interface {
	Emit() ([]byte, error)
	String() string
}

// BaseEvent is an event that is sent when the pipeline has been started or exited
type BaseEvent struct {
	Ts        int64             `json:"ts"`
	Kind      string            `json:"name"`
	Version   string            `json:"version,omitempty"`
	Endpoints map[string]string `json:"endpoints,omitempty"`
}

// NewBootEvent (surprisingly) creates a new baseEvent
func NewBootEvent(ts int64, version string, endpoints map[string]string) *BaseEvent {
	e := &BaseEvent{
		Ts:        ts,
		Kind:      "boot",
		Version:   version,
		Endpoints: endpoints,
	}
	return e
}

// NewExitEvent (surprisingly) creates a new BaseEvent
func NewExitEvent(ts int64, version string, endpoints map[string]string) *BaseEvent {
	e := &BaseEvent{
		Ts:        ts,
		Kind:      "exit",
		Version:   version,
		Endpoints: endpoints,
	}
	return e
}

// Emit prepares the event to be emitted and marshalls the event into an json
func (e *BaseEvent) Emit() ([]byte, error) {
	return json.Marshal(e)
}

// String
func (e *BaseEvent) String() string {
	msg := fmt.Sprintf("%s", e.Kind)
	msg += fmt.Sprintf(" %v", e.Endpoints)
	return msg
}

// MetricsEvent is an event used to indicated progress.
type MetricsEvent struct {
	Ts   int64  `json:"ts"`
	Kind string `json:"name"`
	Path string `json:"path"`

	// Records indicated the total number of documents that have been transmitted
	Records int `json:"records"`
}

// NewMetricsEvent creates a new metrics event
func NewMetricsEvent(ts int64, path string, records int) *MetricsEvent {
	e := &MetricsEvent{
		Ts:      ts,
		Kind:    "metrics",
		Path:    path,
		Records: records,
	}
	return e
}

// Emit prepares the event to be emitted and marshalls the event into an json
func (e *MetricsEvent) Emit() ([]byte, error) {
	return json.Marshal(e)
}

func (e *MetricsEvent) String() string {
	msg := fmt.Sprintf("%s %s", e.Kind, e.Path)
	msg += fmt.Sprintf(" records: %d", e.Records)
	return msg
}

// ErrorEvent is an event that indicates an error occured
// during the processing of a pipeline
type ErrorEvent struct {
	Ts   int64  `json:"ts"`
	Kind string `json:"name"`
	Path string `json:"path"`

	// Record is the document (if any) that was in progress when the error occured
	Record interface{} `json:"record,omitempty"`

	// Message is the error message as a string
	Message string `json:"message,omitempty"`
}

// NewErrorEvent are events sent to indicate a problem processing on one of the nodes
func NewErrorEvent(ts int64, path string, record interface{}, message string) *ErrorEvent {
	e := &ErrorEvent{
		Ts:      ts,
		Kind:    "error",
		Path:    path,
		Record:  record,
		Message: message,
	}
	return e
}

// Emit prepares the event to be emitted and marshalls the event into an json
func (e *ErrorEvent) Emit() ([]byte, error) {
	return json.Marshal(e)
}

// String
func (e *ErrorEvent) String() string {
	msg := fmt.Sprintf("%s", e.Kind)
	msg += fmt.Sprintf(" record: %v, message: %s", e.Record, e.Message)
	return msg
}
