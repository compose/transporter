package adaptor

import (
	"sync"
	"testing"
	"time"

	"github.com/compose/transporter/client"
)

var (
	_ Adaptor = &Mock{}
	_ Adaptor = &UnsupportedMock{}
)

// Mock can be used for mocking tests that need no functional client interfaces.
type Mock struct {
	BaseConfig
}

// Client satisfies the V2 interface for providing a client.Client.
func (m *Mock) Client() (client.Client, error) {
	return &client.Mock{}, nil
}

// Reader satisfies the V2 interface for providing a client.Reader.
func (m *Mock) Reader() (client.Reader, error) {
	return &client.MockReader{}, nil
}

// Writer satisfies the V2 interface for providing a client.Writer.
func (m *Mock) Writer(chan struct{}, *sync.WaitGroup) (client.Writer, error) {
	return &client.MockWriter{}, nil
}

// MockClientErr can be used to to mock client.Client() errors.
type MockClientErr struct {
	BaseConfig
}

// Client satisfies the client.Client interface.
func (m *MockClientErr) Client() (client.Client, error) {
	return &client.MockErr{}, nil
}

// Reader satisfies client.Reader.
func (m *MockClientErr) Reader() (client.Reader, error) {
	return &client.MockReader{}, nil
}

// Writer satisfies client.Writer.
func (m *MockClientErr) Writer(chan struct{}, *sync.WaitGroup) (client.Writer, error) {
	return &client.MockWriter{}, nil
}

// MockWriterErr can be used to to mock client.Write() errors.
type MockWriterErr struct {
	BaseConfig
}

// Client satisfies the client.Client interface.
func (m *MockWriterErr) Client() (client.Client, error) {
	return &client.Mock{}, nil
}

// Reader satisfies the V2 interface for providing a client.Reader.
func (m *MockWriterErr) Reader() (client.Reader, error) {
	return &client.MockReader{}, nil
}

// Writer satisfies the V2 interface for providing a client.Writer.
func (m *MockWriterErr) Writer(chan struct{}, *sync.WaitGroup) (client.Writer, error) {
	return &client.MockErrWriter{}, nil
}

// UnsupportedMock can be used for mocking tests that need no functional client interfaces.
type UnsupportedMock struct {
	BaseConfig
}

// Client satisfies the V2 interface for providing a client.Client.
func (m *UnsupportedMock) Client() (client.Client, error) {
	return nil, ErrFuncNotSupported{"unsupported", "Client()"}
}

// Reader satisfies the V2 interface for providing a client.Reader.
func (m *UnsupportedMock) Reader() (client.Reader, error) {
	return nil, ErrFuncNotSupported{"unsupported", "Reader()"}
}

// Writer satisfies the V2 interface for providing a client.Writer.
func (m *UnsupportedMock) Writer(chan struct{}, *sync.WaitGroup) (client.Writer, error) {
	return nil, ErrFuncNotSupported{"unsupported", "Writer()"}
}

// MockConfirmWrites is a helper function for tests needing a confirms chan.
func MockConfirmWrites() (chan struct{}, func() bool) {
	confirms := make(chan struct{})
	done := make(chan struct{})
	var confirmed bool
	go func() {
		for {
			select {
			case <-confirms:
				confirmed = true
			case <-done:
				return
			}
		}
	}()
	return confirms, func() bool { close(done); return confirmed }
}

// VerifyWriteConfirmed is a helper function to be used in conjunction with
// MockConfirmWrites.
func VerifyWriteConfirmed(f func() bool, t *testing.T) {
	time.Sleep(100 * time.Millisecond)
	if confirmed := f(); !confirmed {
		t.Errorf("writes were not confirmed but should have been")
	}
}
