package opfilter

import (
	"strings"

	"github.com/compose/transporter/function"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

var (
	_ function.Function = &opfilter{}
)

func init() {
	function.Add(
		"opfilter",
		func() function.Function {
			return &opfilter{}
		},
	)
}

// opfilter will skipped messages based on the defined filter.
type opfilter struct {
	Whitelist []string `json:"whitelist"`
	Blacklist []string `json:"blacklist"`
}

func (o *opfilter) Apply(msg message.Msg) (message.Msg, error) {
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
