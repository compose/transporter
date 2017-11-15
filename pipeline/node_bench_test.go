package pipeline

import (
	"testing"

	"github.com/compose/transporter/offset"

	"github.com/compose/transporter/message"
)

func BenchmarkNodeWrite(b *testing.B) {
	node, err := NewNodeWithOptions("benchwriter", "test", ".*",
		WithWriteTimeout("500ms"),
	)
	if err != nil {
		b.Error(err)
	}

	msg := &message.Base{}
	off := offset.Offset{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		node.write(msg, off)
	}
}
