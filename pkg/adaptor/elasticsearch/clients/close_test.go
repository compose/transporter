package clients

import (
	"sync"
	"testing"
)

type MockSession struct {
	closeCalled bool
}

func (s *MockSession) Close() {
	s.closeCalled = true
}

func TestCloser(t *testing.T) {
	s := &MockSession{}
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go Close(done, &wg, s)
	close(done)
	wg.Wait()
	if !s.closeCalled {
		t.Error("Close was never called but should have been")
	}
}
