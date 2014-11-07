package node

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
