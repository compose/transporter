package rename

import (
	"github.com/compose/transporter/function"
	"github.com/compose/transporter/message"
)

var (
	_ function.Function = &rename{}
)

func init() {
	function.Add(
		"rename",
		func() function.Function {
			return &rename{}
		},
	)
}

// rename swaps out the field names based on the provided config
type rename struct {
	SwapMap map[string]string `json:"field_map"`
}

func (r *rename) Apply(msg message.Msg) (message.Msg, error) {
	for oldName, newName := range r.SwapMap {
		if val, ok := msg.Data().AsMap()[oldName]; ok {
			msg.Data().Set(newName, val)
			msg.Data().Delete(oldName)
		}
	}
	return msg, nil
}
