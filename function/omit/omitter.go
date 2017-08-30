package omit

import (
	"github.com/compose/transporter/function"
	"github.com/compose/transporter/message"
)

var (
	_ function.Function = &omitter{}
)

func init() {
	function.Add(
		"omit",
		func() function.Function {
			return &omitter{}
		},
	)
}

type omitter struct {
	Fields []string `json:"fields"`
}

func (o *omitter) Apply(msg message.Msg) (message.Msg, error) {
	for _, k := range o.Fields {
		msg.Data().Delete(k)
	}
	return msg, nil
}
