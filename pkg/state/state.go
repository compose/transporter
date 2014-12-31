package state

import (
	"github.com/compose/transporter/pkg/message"
)

type SessionStore interface {
	Set(path string, msg *message.Msg) error
	Get(path string) (*message.Msg, error)
}
