package offset

import (
	"sync"
	"time"
)

var (
	_ Manager = &MockManager{}
)

// MockManager implements offset.Manager for use in tests.
type MockManager struct {
	MemoryMap   map[string]uint64
	CommitDelay time.Duration
	CommitErr   error
	sync.Mutex
}

// CommitOffset satisfies offset.Manager interface.
func (m *MockManager) CommitOffset(o Offset, override bool) error {
	if m.CommitErr != nil {
		return m.CommitErr
	}
	if m.CommitDelay > 0 {
		go func() {
			time.Sleep(m.CommitDelay)
			m.Lock()
			defer m.Unlock()
			if currentOffset, ok := m.MemoryMap[o.Namespace]; ok && currentOffset >= o.LogOffset {
				return
			}
			m.MemoryMap[o.Namespace] = o.LogOffset
		}()
		return nil
	}
	m.Lock()
	defer m.Unlock()
	if currentOffset, ok := m.MemoryMap[o.Namespace]; ok && currentOffset >= o.LogOffset {
		return nil
	}
	m.MemoryMap[o.Namespace] = o.LogOffset
	return nil
}

// OffsetMap satisfies offset.Manager interface.
func (m *MockManager) OffsetMap() map[string]uint64 {
	m.Lock()
	defer m.Unlock()
	return m.MemoryMap
}

// NewestOffset satisfies offset.Manager interface.
func (m *MockManager) NewestOffset() int64 {
	m.Lock()
	defer m.Unlock()
	if len(m.MemoryMap) == 0 {
		return -1
	}
	var newestOffset uint64
	for _, v := range m.MemoryMap {
		if newestOffset < v {
			newestOffset = v
		}
	}
	return int64(newestOffset)
}
