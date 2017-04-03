package commitlog

// LogEntry represents the high level representation of the message portion of each entry in the commit log.
type LogEntry struct {
	Key       []byte
	Value     []byte
	Timestamp uint64
	Mode      Mode
}

// Mode is a representation of where a in the process a reader is with respect to a given namespace.
type Mode int

// currently supported Modes are Copy, Sync, and Complete
const (
	Copy Mode = iota
	Sync
	Complete
)
