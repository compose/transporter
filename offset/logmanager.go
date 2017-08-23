package offset

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

var (
	_ Manager = &LogManager{}
)

// LogManager provides writers the ability to track offsets associated with processed messages.
type LogManager struct {
	log   *commitlog.CommitLog
	name  string
	nsMap map[string]uint64
	sync.Mutex
}

// NewLogManager creates a new instance of LogManager and initializes its namespace map by reading any
// existing log files.
func NewLogManager(path, name string) (*LogManager, error) {
	m := &LogManager{
		name:  name,
		nsMap: make(map[string]uint64),
	}

	l, err := commitlog.New(
		commitlog.WithPath(filepath.Join(path, fmt.Sprintf("%s-%s", offsetPrefixDir, name))),
		commitlog.WithMaxSegmentBytes(1024*1024*1024),
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

func (m *LogManager) buildMap() error {
	var lastError error
	for _, s := range m.log.Segments() {
		// s.Open()
		var readPosition int64
		for {
			// skip the offsetHeader
			readPosition += offsetHeaderLen

			keyLenBytes := make([]byte, 4)
			_, lastError = s.ReadAt(keyLenBytes, readPosition)
			if lastError != nil && lastError == io.EOF {
				break
			} else if lastError != nil {
				return lastError
			}
			keyLen := encoding.Uint32(keyLenBytes)
			readPosition += 4

			// now we read the namespace based on the length
			nsBytes := make([]byte, keyLen)
			_, lastError = s.ReadAt(nsBytes, readPosition)
			if lastError != nil {
				break
			}
			// we can add 4 here since we know the size of the value is 8 bytes
			readPosition += int64(keyLen) + 4
			// we can cheat here since we know the value will always be the 8-byte offset
			valBytes := make([]byte, 8)
			_, lastError = s.ReadAt(valBytes, readPosition)
			if lastError != nil {
				break
			}
			readPosition += 8
			m.nsMap[string(nsBytes)] = encoding.Uint64(valBytes)
		}
	}
	return lastError
}

// CommitOffset verifies it does not contain an offset older than the current offset
// and persists to the log.
func (m *LogManager) CommitOffset(o Offset, override bool) error {
	m.Lock()
	defer m.Unlock()
	if currentOffset, ok := m.nsMap[o.Namespace]; !override && ok && currentOffset >= o.LogOffset {
		log.With("currentOffest", currentOffset).
			With("providedOffset", o.LogOffset).
			Debugln("refusing to commit offset")
		return nil
	}
	_, err := m.log.Append(o.Bytes())
	if err != nil {
		return err
	}
	m.nsMap[o.Namespace] = o.LogOffset
	return nil
}

// OffsetMap provides access to the underlying map containing the newest offset for every
// namespace.
func (m *LogManager) OffsetMap() map[string]uint64 {
	m.Lock()
	defer m.Unlock()
	return m.nsMap
}

// NewestOffset loops over every offset and returns the highest one.
func (m *LogManager) NewestOffset() int64 {
	m.Lock()
	defer m.Unlock()
	if len(m.nsMap) == 0 {
		return -1
	}
	var newestOffset uint64
	for _, v := range m.nsMap {
		if newestOffset < v {
			newestOffset = v
		}
	}
	return int64(newestOffset)
}
