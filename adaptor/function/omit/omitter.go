package omit

import (
	"sync"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/message"
)

func init() {
	adaptor.Add(
		"omit",
		func() adaptor.Adaptor {
			return &Omitter{}
		},
	)
}

type Omitter struct {
	Fields []string `json:"fields"`
}

func (o *Omitter) Client() (client.Client, error) {
	return &client.Mock{}, nil
}

func (o *Omitter) Reader() (client.Reader, error) {
	return nil, adaptor.ErrFuncNotSupported{Name: "transformer", Func: "Reader()"}
}

func (o *Omitter) Writer(chan struct{}, *sync.WaitGroup) (client.Writer, error) {
	return o, nil
}

func (o *Omitter) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		for _, k := range o.Fields {
			msg.Data().Delete(k)
		}
		return msg, nil
	}
}
