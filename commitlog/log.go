package commitlog

import (
	"github.com/compose/transporter/message/ops"
)

const (
	offsetPos         = 0
	sizePos           = 8
	tsPos             = 12
	attrPos           = 20
	logEntryHeaderLen = 21

	modeMask = 3
	opMask   = 28
	opShift  = 2
)

// LogEntry represents the high level representation of the message portion of each entry in the commit log.
type LogEntry struct {
	Key       []byte
	Value     []byte
	Timestamp uint64
	Mode      Mode
	Op        ops.Op
}

// KeyValueToBytes combines the Key and Value into a single []byte and includes the size of each in the first 4 bytes
// preceding the data.
func (le LogEntry) KeyValueToBytes() []byte {
	keyBytes := make([]byte, len(le.Key)+4)
	encoding.PutUint32(keyBytes[0:4], uint32(len(le.Key)))
	copy(keyBytes[4:], le.Key)

	valBytes := make([]byte, len(le.Value)+4)
	encoding.PutUint32(valBytes[0:4], uint32(len(le.Value)))
	copy(valBytes[4:], le.Value)
	return append(keyBytes, valBytes...)
}

// ModeOpToByte converts the Mode and Op values into a single byte by performing bitwise operations.
// Mode is stored in bits 0 - 1
// Op is stored in bits 2 - 4
// bits 5 - 7 are currently unused
func (le LogEntry) ModeOpToByte() byte {
	return byte(int(le.Mode) | (int(le.Op) << opShift))
}

// Mode is a representation of where a in the process a reader is with respect to a given namespace.
type Mode int

// currently supported Modes are Copy, Sync, and Complete
const (
	Copy Mode = iota
	Sync
	Complete
)

// Log is a alias type for []byte.
type Log []byte

// NewLogFromEntry take the offset and LogEntry and builds the underlying []byte to be stored.
func NewLogFromEntry(offset uint64, le LogEntry) Log {
	l := make([]byte, logEntryHeaderLen)
	encoding.PutUint64(l[offsetPos:sizePos], offset)

	encoding.PutUint64(l[tsPos:attrPos], le.Timestamp)

	l[attrPos] = le.ModeOpToByte()

	kvBytes := le.KeyValueToBytes()
	l = append(l, kvBytes...)
	size := uint32(len(kvBytes))
	encoding.PutUint32(l[sizePos:tsPos], size)
	return l
}
