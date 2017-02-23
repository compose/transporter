package file

import (
	"encoding/json"
	"io"

	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

var (
	_ client.Reader = &Reader{}
)

// Reader implements the behavior defined by client.Reader for interfacing with the file.
type Reader struct{}

func newReader() client.Reader {
	return &Reader{}
}

func (r *Reader) Read(filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan message.Msg, error) {
		out := make(chan message.Msg)
		session := s.(*Session)
		go func() {
			defer close(out)
			results := r.decodeFile(session, done)
			for {
				select {
				case <-done:
					return
				case result, ok := <-results:
					if !ok {
						log.With("file", session.file.Name()).Infoln("Read completed")
						return
					}
					if filterFn(session.file.Name()) {
						out <- message.From(ops.Insert, session.file.Name(), result)
					}
				}
			}
		}()

		return out, nil
	}
}

func (r *Reader) decodeFile(s *Session, done chan struct{}) chan data.Data {
	out := make(chan data.Data)
	go func() {
		defer close(out)
		dec := json.NewDecoder(s.file)
		for {
			var doc = make(data.Data)
			if err := dec.Decode(&doc); err == io.EOF {
				return
			} else if err != nil {
				log.With("file", s.file.Name()).Errorf("Can't unmarshal document (%v)", err)
				continue
			}
			out <- doc
		}
	}()
	return out
}
