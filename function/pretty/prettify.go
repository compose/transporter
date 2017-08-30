package pretty

import (
	"encoding/json"
	"strings"

	"github.com/compose/mejson"
	"github.com/compose/transporter/function"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
)

const (
	defaultIndent = 2
)

var (
	defaultPrettifier = &prettify{Spaces: defaultIndent}
)

func init() {
	function.Add(
		"pretty",
		func() function.Function {
			return defaultPrettifier
		},
	)
}

type prettify struct {
	Spaces int `json:"spaces"`
}

func (p *prettify) Apply(msg message.Msg) (message.Msg, error) {
	d, _ := mejson.Unmarshal(msg.Data())
	b, _ := json.Marshal(d)
	if p.Spaces > 0 {
		b, _ = json.MarshalIndent(d, "", strings.Repeat(" ", p.Spaces))
	}
	log.Infof("\n%s", string(b))
	return msg, nil
}
