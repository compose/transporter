package pipe

import "github.com/compose/transporter/message"

// Mock serves as a pipe used to count the number of messages sent to writeMessage.
type Mock struct {
	MsgCount int
}

// WriteMessage increments the message counter.
func (m *Mock) WriteMessage(msg message.Msg) (message.Msg, error) {
	m.MsgCount++
	return msg, nil
}
