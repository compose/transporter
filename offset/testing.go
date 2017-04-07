package offset

import (
	"sync"
	"time"
)

var (
	_ Manager = &MockManager{}
)

type MockManager struct {
	MemoryMap   map[string]uint64
	CommitDelay time.Duration
	sync.Mutex
}

func (m *MockManager) CommitOffset(o Offset) error {
	if m.CommitDelay > 0 {
		go func() {
			m.Lock()
			defer m.Unlock()
			time.Sleep(m.CommitDelay)
			m.MemoryMap[o.Namespace] = o.LogOffset
		}()
		return nil
	}
	m.Lock()
	defer m.Unlock()
	m.MemoryMap[o.Namespace] = o.LogOffset
	return nil
}

func (m *MockManager) OffsetMap() map[string]uint64 {
	m.Lock()
	defer m.Unlock()
	return m.MemoryMap
}

func (m *MockManager) NewestOffset() int64 {
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
