package pretty

import (
	"encoding/json"
	"strings"
	"sync"

	"github.com/compose/mejson"
	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
)

const (
	DefaultIndent = 2
)

var (
	DefaultPrettifier = &Prettify{Spaces: DefaultIndent}
)

func init() {
	adaptor.Add(
		"pretty",
		func() adaptor.Adaptor {
			return DefaultPrettifier
		},
	)
}

type Prettify struct {
	Spaces int `json:"spaces"`
}

func (p *Prettify) Client() (client.Client, error) {
	return &client.Mock{}, nil
}

func (p *Prettify) Reader() (client.Reader, error) {
	return nil, adaptor.ErrFuncNotSupported{Name: "transformer", Func: "Reader()"}
}

func (p *Prettify) Writer(chan struct{}, *sync.WaitGroup) (client.Writer, error) {
	return p, nil
}

func (p *Prettify) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		d, _ := mejson.Unmarshal(msg.Data())
		b, _ := json.Marshal(d)
		if p.Spaces > 0 {
			b, _ = json.MarshalIndent(d, "", strings.Repeat(" ", p.Spaces))
		}
		log.Infof("\n%s", string(b))
		return msg, nil
	}
}
