package state

import (
	"github.com/compose/transporter/pkg/message"
)

type MsgState struct {
	Id        string
	Timestamp int64
}

type SessionStore interface {
	Set(path string, msg *message.Msg) error
	Get(path string) (string, int64, error)
}
