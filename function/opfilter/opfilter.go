package opfilter

import (
	"strings"

	"github.com/compose/transporter/function"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

var (
	_ function.Function = &Opfilter{}
)

func init() {
	function.Add(
		"opfilter",
		func() function.Function {
			return &Opfilter{}
		},
	)
}

// Opfilter will skipped messages based on the defined filter.
type Opfilter struct {
	Whitelist []string `json:"whitelist"`
	Blacklist []string `json:"blacklist"`
}

// Apply changes the incoming namespace to a new one if it's been defined in the config.
func (o *Opfilter) Apply(msg message.Msg) (message.Msg, error) {
	if len(o.Whitelist) > 0 && !isOpInList(msg.OP(), o.Whitelist) {
		return nil, nil
	} else if len(o.Blacklist) > 0 && isOpInList(msg.OP(), o.Blacklist) {
		return nil, nil
	}
	return msg, nil
}

func isOpInList(op ops.Op, list []string) bool {
	for _, listedOp := range list {
		if ops.OpTypeFromString(strings.ToLower(listedOp)) == op {
			return true
		}
	}
	return false
}
