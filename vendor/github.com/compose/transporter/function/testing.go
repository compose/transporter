package function

import (
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
)

var (
	_ Function = &Mock{}
)

// Mock can be used in tests to track whether Function.Apply was called
// and test for expected errors.
type Mock struct {
	ApplyCount int
	Err        error
}

// Apply fulfills the Function interface for use in tests.
func (m *Mock) Apply(msg message.Msg) (message.Msg, error) {
	m.ApplyCount++
	log.With("apply_count", m.ApplyCount).With("err", m.Err).Debugln("applying...")
	return msg, m.Err
}
