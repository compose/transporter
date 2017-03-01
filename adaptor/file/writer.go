package file

import (
	"encoding/json"
	"os"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/message"
)

var _ client.Writer = &Writer{}

// Writer implements client.Writer for use with Files
type Writer struct{}

func newWriter() *Writer {
	w := &Writer{}
	return w
}

func (w *Writer) Write(msg message.Msg) func(client.Session) error {
	return func(s client.Session) error {
		return dumpMessage(msg, s.(*Session).file)
	}
}

func dumpMessage(msg message.Msg, f *os.File) error {
	return json.NewEncoder(f).Encode(msg.Data())
}
