package commitlog

import (
	"io"

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

// ModeOpToByte converts the Mode and Op values into a single byte by performing bitwise operations.
// Mode is stored in bits 0 - 1
// Op is stored in bits 2 - 4
// bits 5 - 7 are currently unused
func (le LogEntry) ModeOpToByte() byte {
	return byte(int(le.Mode) | (int(le.Op) << opShift))
}

func ReadEntry(r io.Reader) (uint64, LogEntry, error) {
	header := make([]byte, logEntryHeaderLen)
	if _, err := r.Read(header); err != nil {
		return 0, LogEntry{}, err
	}
	k, v, err := readKeyValue(encoding.Uint32(header[sizePos:tsPos]), r)
	if err != nil {
		return 0, LogEntry{}, err
	}
	l := LogEntry{
		Key:       k,
		Value:     v,
		Timestamp: encoding.Uint64(header[tsPos:attrPos]),
		Mode:      modeFromBytes(header),
		Op:        opFromBytes(header),
	}
	return encoding.Uint64(header[offsetPos:sizePos]), l, nil
}

// readKeyValue returns the key and value stored given the size and io.Reader.
func readKeyValue(size uint32, r io.Reader) ([]byte, []byte, error) {
	kvBytes := make([]byte, size)
	if _, err := r.Read(kvBytes); err != nil {
		return nil, nil, err
	}
	keyLen := encoding.Uint32(kvBytes[0:4])
	// we can grab the key from keyLen and the we know the value is stored
	// after the keyLen + 8 (4 byte size of key and value)
	return kvBytes[4 : keyLen+4], kvBytes[keyLen+8:], nil
}

func modeFromBytes(b []byte) Mode {
	return Mode(b[attrPos] & modeMask)
}
func opFromBytes(b []byte) ops.Op {
	return ops.Op(b[attrPos] & opMask >> opShift)
}

// Mode is a representation of where a in the process a reader is with respect to a given namespace.
type Mode int

// currently supported Modes are Copy, Sync, and Complete
const (
	Copy Mode = iota
	Sync
	Complete
)

// NewLogFromEntry takes the LogEntry and builds the underlying []byte to be stored.
func NewLogFromEntry(le LogEntry) Log {
	keyLen := len(le.Key)
	valLen := len(le.Value)
	kvLen := keyLen + valLen + 8
	l := make([]byte, logEntryHeaderLen+kvLen)

	encoding.PutUint64(l[tsPos:attrPos], le.Timestamp)

	l[attrPos] = le.ModeOpToByte()

	kvPosition := logEntryHeaderLen + 4
	encoding.PutUint32(l[logEntryHeaderLen:kvPosition], uint32(keyLen))
	copy(l[kvPosition:kvPosition+keyLen], le.Key)

	encoding.PutUint32(l[kvPosition+keyLen:kvPosition+keyLen+4], uint32(valLen))
	copy(l[kvPosition+keyLen+4:], le.Value)

	encoding.PutUint32(l[sizePos:tsPos], uint32(kvLen))
	return l
}
