package function

import (
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
)

var (
	_ Function = &Mock{}
)

type Mock struct {
	ApplyCount int
	Err        error
}

func (m *Mock) Apply(msg message.Msg) (message.Msg, error) {
	m.ApplyCount++
	log.With("apply_count", m.ApplyCount).With("err", m.Err).Debugln("applying...")
	return msg, m.Err
}
