package transformer

import (
	"time"

	"git.compose.io/compose/transporter/pkg/message"
	"git.compose.io/compose/transporter/pkg/message/data"
	"git.compose.io/compose/transporter/pkg/message/ops"
)

type Adaptor struct {
}

func init() {
	a := Adaptor{}
	message.Register(a.Name(), a)
}

func (r Adaptor) Name() string {
	return "transformer"
}

func (r Adaptor) From(op ops.Op, namespace string, d data.Data) message.Msg {
	return &Message{
		Operation: op,
		TS:        time.Now().Unix(),
		NS:        namespace,
		MapData:   d,
	}
}
