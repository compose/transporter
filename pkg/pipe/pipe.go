package pipe

import (
	"github.com/MongoHQ/transporter/pkg/message"
)

type Pipe chan *message.Msg
