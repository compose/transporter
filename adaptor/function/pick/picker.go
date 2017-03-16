package pick

import (
	"sync"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/message"
)

func init() {
	adaptor.Add(
		"pick",
		func() adaptor.Adaptor {
			return &Picker{}
		},
	)
}

type Picker struct {
	Fields []string `json:"fields"`
}

func (p *Picker) Client() (client.Client, error) {
	return &client.Mock{}, nil
}

func (p *Picker) Reader() (client.Reader, error) {
	return nil, adaptor.ErrFuncNotSupported{Name: "transformer", Func: "Reader()"}
}

func (p *Picker) Writer(chan struct{}, *sync.WaitGroup) (client.Writer, error) {
	return p, nil
}

func (p *Picker) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		pluckedMsg := map[string]interface{}{}
		for _, k := range p.Fields {
			if v, ok := msg.Data().AsMap()[k]; ok {
				pluckedMsg[k] = v
			}
		}
		return message.From(msg.OP(), msg.Namespace(), pluckedMsg), nil
	}
}
