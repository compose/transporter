package offsetmanager

import (
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"github.com/compose/transporter/commitlog"
	"github.com/compose/transporter/log"
)

const (
	offsetPrefixDir = "__consumer_offsets"
)

// Manager provides writers the ability to track offsets associated with processed messages.
type Manager struct {
	log   *commitlog.CommitLog
	name  string
	nsMap map[string]uint64
	sync.Mutex
}

// New creates a new instance of Manager and initializes its namespace map by reading any
// existing log files.
func New(path, name string) (*Manager, error) {
	m := &Manager{
		name:  name,
		nsMap: make(map[string]uint64),
	}

	l, err := commitlog.New(
		commitlog.WithPath(filepath.Join(path, fmt.Sprintf("%s-%s", offsetPrefixDir, name))),
	)
	if err != nil {
		return nil, err
	}
	m.log = l

	err = m.buildMap()
	if err == io.EOF {
		return m, nil
	}

	return m, err
}

func (m *Manager) buildMap() error {
	var readPosition int64
	for _, s := range m.log.Segments() {
		// skip the offsetHeader
		readPosition += offsetHeaderLen

		keyLenBytes := make([]byte, 4)
		_, err := s.ReadAt(keyLenBytes, readPosition)
		if err != nil {
			return err
		}
		keyLen := encoding.Uint32(keyLenBytes)
		readPosition += 4

		// now we read the namespace based on the length
		nsBytes := make([]byte, keyLen)
		_, err = s.ReadAt(nsBytes, readPosition)
		if err != nil {
			return err
		}
		// we can add 4 here since we know the size of the value is 8 bytes
		readPosition += int64(keyLen) + 4
		// we can cheat here since we know the value will always be the 8-byte offset
		valBytes := make([]byte, 8)
		_, err = s.ReadAt(valBytes, readPosition)
		if err != nil {
			return err
		}
		readPosition += 8
		m.nsMap[string(nsBytes)] = encoding.Uint64(valBytes)
	}
	return nil
}

// CommitOffset verifies it does not contain an offset older than the current offset
// and persists to the log.
func (m *Manager) CommitOffset(o Offset) error {
	m.Lock()
	defer m.Unlock()
	if currentOffset, ok := m.nsMap[o.Namespace]; ok && currentOffset >= o.Offset {
		log.With("currentOffest", currentOffset).
			With("providedOffset", o.Offset).
			Infoln("refusing to commit offset")
		return nil
	}
	_, err := m.log.Append(o.Bytes())
	if err != nil {
		return err
	}
	m.nsMap[o.Namespace] = o.Offset
	return nil
}

// OffsetMap provides access to the underlying map containing the newest offset for every
// namespace.
func (m *Manager) OffsetMap() map[string]uint64 {
	m.Lock()
	defer m.Unlock()
	return m.nsMap
}
