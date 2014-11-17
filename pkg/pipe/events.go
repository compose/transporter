package pipe

import (
	"encoding/json"
	"time"
)

type eventKind int

func (e eventKind) String() string {
	switch e {
	case bootKind:
		return "boot"
	case errorKind:
		return "error"
	case metricsKind:
		return "metrics"
	case exitKind:
		return "exit"
	}
	return ""
}

const (
	bootKind eventKind = iota
	errorKind
	metricsKind
	exitKind
)

// an Event is produced periodically by the running transporter.
//
// Events come in multiple kinds.  BootEvents are emitted when the transporter starts,
// MetricsEvents are emittied by each pipe and include a measure of how many messages have been processed
type Event struct {
	Ts           int64  `json:"ts"`
	Kind         string `json:"event"`
	bootEvent    `json:",omitempty"`
	metricsEvent `json:",omitempty"`
}

func (e Event) String() string {
	ba, _ := json.Marshal(e)
	return string(ba)
}

// bootevents are sent when the pipeline has been started
type bootEvent struct {
	Version   string            `json:"version,omitempty"`
	Endpoints map[string]string `json:"endpoints,omitempty"`
}

// NewBootEvent (surprisingly) creates a new bootevent
func NewBootEvent(ts int64, version string, endpoints map[string]string) Event {
	e := Event{Ts: ts, Kind: bootKind.String()}
	e.Version = version
	e.Endpoints = endpoints
	return e
}

// Metrics events are sent by the nodes periodically
type metricsEvent struct {
	Path       string `json:"path,omitempty"`
	RecordsIn  int    `json:"records_in,omitempty"`
	RecordsOut int    `json:"records_out,omitempty"`
}

// newMetricsEvent creates a new metrics event
func NewMetricsEvent(ts int64, path string, in, out int) Event {
	e := Event{Ts: ts, Kind: metricsKind.String()}
	e.Path = path
	e.RecordsIn = in
	e.RecordsOut = out
	return e
}

// NewExitEvent (surprisingly) creates a new exitevent
func NewExitEvent(ts int64, version string, endpoints map[string]string) Event {
	e := Event{Ts: ts, Kind: exitKind.String()}
	e.Version = version
	e.Endpoints = endpoints
	return e
}

//
// lets keep track of metrics on a nodeimpl, and send them out periodically to our event chan
type nodeMetrics struct {
	ticker     *time.Ticker
	eChan      chan Event
	path       string
	RecordsIn  int
	RecordsOut int
}

// NewNodeMetrics creates a new nodeMetrics, and starts a ticker that will emit a new metrics event every interval.
func NewNodeMetrics(path string, eventChan chan Event, interval time.Duration) *nodeMetrics {
	m := &nodeMetrics{path: path, eChan: eventChan}

	// if we have a non zero interval then spawn a ticker to send metrics out the channel
	if interval > 0 {
		m.ticker = time.NewTicker(interval)
		go func() {
			for _ = range m.ticker.C {
				m.send()
			}
		}()
	}
	return m
}

// send a new metrics event on the event channel
func (m *nodeMetrics) send() {
	m.eChan <- NewMetricsEvent(time.Now().Unix(), m.path, m.RecordsIn, m.RecordsOut)
}

// Stop stops the ticker that sends out new metrics and broadcast a final metric for the node.
// This shuts down the nodeMetrics.
func (m *nodeMetrics) Stop() {
	if m.ticker != nil {
		m.ticker.Stop()
	}
	m.send()
}
