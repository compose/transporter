package remap

import (
	"github.com/compose/transporter/function"
	"github.com/compose/transporter/message"
)

var (
	_ function.Function = &Remap{}
)

func init() {
	function.Add(
		"remap",
		func() function.Function {
			return &Remap{}
		},
	)
}

// Remap swaps out the namespaces based on the provided config
type Remap struct {
	SwapMap map[string]string `json:"ns_map"`
}

// Apply changes the incoming namespace to a new one if it's been defined in the config.
func (r *Remap) Apply(msg message.Msg) (message.Msg, error) {
	if ns, ok := r.SwapMap[msg.Namespace()]; ok {
		msg.UpdateNamespace(ns)
	}
	return msg, nil
}
