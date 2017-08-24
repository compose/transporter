package pick

import (
	"github.com/compose/transporter/function"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
)

var (
	_ function.Function = &picker{}
)

func init() {
	function.Add(
		"pick",
		func() function.Function {
			return &picker{}
		},
	)
}

type picker struct {
	Fields []string `json:"fields"`
}

func (p *picker) Apply(msg message.Msg) (message.Msg, error) {
	log.With("msg", msg).Debugln("picking...")
	pluckedMsg := map[string]interface{}{}
	for _, k := range p.Fields {
		if v, ok := msg.Data().AsMap()[k]; ok {
			pluckedMsg[k] = v
		}
	}
	log.With("msg", pluckedMsg).Debugln("...picked")
	return message.From(msg.OP(), msg.Namespace(), pluckedMsg), nil
}
