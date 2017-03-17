package omit

import (
	"github.com/compose/transporter/function"
	"github.com/compose/transporter/message"
)

func init() {
	function.Add(
		"omit",
		func() function.Function {
			return &Omitter{}
		},
	)
}

type Omitter struct {
	Fields []string `json:"fields"`
}

func (o *Omitter) Apply(msg message.Msg) (message.Msg, error) {
	for _, k := range o.Fields {
		msg.Data().Delete(k)
	}
	return msg, nil
}
