package state

import (
	"git.compose.io/compose/transporter/pkg/message"
	"git.compose.io/compose/transporter/pkg/message/data"
)

// MsgState encapsulates a message state to be stored in the SessionStore for
// graceful recovery
type MsgState struct {
	Msg   message.Msg
	Extra data.Data
}

// SessionStore describes the interface for a store for transporter MsgStates
// to be used graceful recovery
type SessionStore interface {
	Set(path string, state *MsgState) error
	Get(path string) (*MsgState, error)
}
