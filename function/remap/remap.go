package remap

import (
	"github.com/compose/transporter/function"
	"github.com/compose/transporter/message"
)

var (
	_ function.Function = &remap{}
)

func init() {
	function.Add(
		"remap",
		func() function.Function {
			return &remap{}
		},
	)
}

// remap swaps out the namespaces based on the provided config
type remap struct {
	SwapMap map[string]string `json:"ns_map"`
}

func (r *remap) Apply(msg message.Msg) (message.Msg, error) {
	if ns, ok := r.SwapMap[msg.Namespace()]; ok {
		msg.UpdateNamespace(ns)
	}
	return msg, nil
}
