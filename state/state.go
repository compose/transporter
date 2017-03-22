package state

// State contains the information a client.Reader attachs to each message.Msg it sends down
// the pipeline.
type State struct {
	Identifier interface{}
	Timestamp  uint64
	Namespace  string
	Mode       Mode
}

// Mode is a decorator for int
type Mode int

// currently supported Modes are Copy and Sync
const (
	Copy Mode = iota
	Sync
)
