package adaptor

import (
	"sync"

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
