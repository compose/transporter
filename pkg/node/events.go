package node

import (
	"encoding/json"
	"time"
)

type EventKind int

func (e EventKind) String() string {
	switch e {
	case BootKind:
		return "boot"
	case ErrorKind:
		return "error"
	case MetricsKind:
		return "metrics"
	}
	return ""
}

const (
	BootKind EventKind = iota
	ErrorKind
	MetricsKind
)

/*
 * Events
 */
type Event struct {
	Ts           int64  `json:"ts"`
	Kind         string `json:"event"`
	BootEvent    `json:",omitempty"`
	MetricsEvent `json:",omitempty"`
}

func (e Event) String() string {
	ba, _ := json.Marshal(e)
	return string(ba)
}

/*
 * bootevents are sent when the pipeline has been started
 */
type BootEvent struct {
	Version   string            `json:"version,omitempty"`
	Endpoints map[string]string `json:"endpoints,omitempty"`
}

func NewBootEvent(ts int64, version string, endpoints map[string]string) Event {
	e := Event{Ts: ts, Kind: BootKind.String()}
	e.Version = version
	e.Endpoints = endpoints
	return e
}

/*
 * Metrics events are sent by the nodes periodically
 */
type MetricsEvent struct {
	Path       string `json:"path,omitempty"`
	RecordsIn  int    `json:"records_in,omitempty"`
	RecordsOut int    `json:"records_out,omitempty"`
}

func NewMetricsEvent(ts int64, path string, in, out int) Event {
	e := Event{Ts: ts, Kind: MetricsKind.String()}
	e.Path = path
	e.RecordsIn = in
	e.RecordsOut = out
	return e
}

/*
 * Event Listener, listens on an event channel
 */
type EventListener struct {
	In chan Event
}

func NewEventListener() EventListener {
	return EventListener{In: make(chan Event)}
}

/*
 * lets keep track of metrics on a nodeimpl, and send them out periodically to our event chan
 */
type NodeMetrics struct {
	ticker     *time.Ticker
	eChan      chan Event
	path       string
	RecordsIn  int
	RecordsOut int
}

func NewNodeMetrics(path string, eventChan chan Event) *NodeMetrics {
	m := &NodeMetrics{path: path, eChan: eventChan}

	m.ticker = time.NewTicker(1 * time.Millisecond)
	go func() {
		for _ = range m.ticker.C {
			m.Send()
		}
	}()
	return m
}

func (m *NodeMetrics) Send() {
	m.eChan <- NewMetricsEvent(time.Now().Unix(), m.path, m.RecordsIn, m.RecordsOut)
}

func (m *NodeMetrics) Stop() {
	m.ticker.Stop()
}
