package etcd

import (
	"github.com/compose/transporter/pkg/client"

	eclient "github.com/coreos/etcd/client"
)

var (
	_ client.Session = &Session{}
)

// Session serves the purpose of wrapping the underlying etcd.Client for use by any client.Reader
// or client.Writer implementations
type Session struct {
	eclient.Client
}

// Close fulfills the client.Session interface
func (s *Session) Close() {
	// no op
}
