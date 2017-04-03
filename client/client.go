package client

import (
	"github.com/compose/transporter/commitlog"
	"github.com/compose/transporter/message"
)

// MessageChanFunc represents the func signature needed to send messages to downstream adaptors.
type MessageChanFunc func(Session, chan struct{}) (chan MessageSet, error)

// MessageSet encapsulates the data being sent down the pipeline and its associated attributes.
type MessageSet struct {
	Msg       message.Msg
	Timestamp int64
	Mode      commitlog.Mode
}

// NsFilterFunc represents the func signature needed to filter while Read()ing.
type NsFilterFunc func(string) bool

// Client provides a standard interface for interacting with the underlying sources/sinks.
type Client interface {
	Connect() (Session, error)
}

// Session represents the connection to the underlying service.
type Session interface {
}

// Closer provides a standard interface for closing a client or session
type Closer interface {
	Close()
}

// Reader represents the ability to send messages down the pipe and is only needed for
// adaptors acting as a Source node.
type Reader interface {
	Read(NsFilterFunc) MessageChanFunc
}

// Writer represents all possible functions needing to be implemented to handle messages.
type Writer interface {
	Write(message.Msg) func(Session) (message.Msg, error)
}

// Write encapsulates the function of determining which function to call based on the msg.OP() and
// also wraps the function call with a Session.
func Write(client Client, writer Writer, msg message.Msg) (message.Msg, error) {
	return sessionFunc(client, writer.Write(msg))
}

func sessionFunc(client Client, op func(Session) (message.Msg, error)) (message.Msg, error) {
	sess, err := client.Connect()
	if err != nil {
		return nil, err
	}
	if s, ok := sess.(Closer); ok {
		defer s.Close()
	}
	return op(sess)
}
