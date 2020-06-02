package offset

import (
	"encoding/binary"

	"github.com/compose/transporter/commitlog"
)

const (
	sizePos         = 8
	timestampPos    = 16
	valLen          = 16
	offsetHeaderLen = 21
)

var (
	encoding = binary.BigEndian
)

// Offset defines the structure a writer needs to supply for tracking.
type Offset struct {
	Namespace string
	LogOffset uint64
	Timestamp int64
}

// Bytes converts Offset to the binary format to be stored on disk.
func (o Offset) Bytes() []byte {
	valBytes := make([]byte, 8)
	encoding.PutUint64(valBytes, o.LogOffset)

	l := commitlog.NewLogFromEntry(commitlog.LogEntry{
		Key:       []byte(o.Namespace),
		Value:     valBytes,
		Timestamp: uint64(o.Timestamp),
	})
	return l
}
