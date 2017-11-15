package rabbitmq

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
	"github.com/streadway/amqp"
)

const (
	// DefaultDeliveryMode is used when writing messages to an exchange.
	DefaultDeliveryMode = amqp.Transient

	// DefaultRoutingKey is set to an empty string so all messages published to the exchange will
	// get routed to whatever queues are bound to it.
	DefaultRoutingKey = ""
)

var (
	_ client.Writer = &Writer{}
)

// Writer implements client.Writer by publishing messages to the cluster based on its configuration.
type Writer struct {
	DeliveryMode uint8
	RoutingKey   string
	KeyInField   bool
}

func (w *Writer) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		if msg.OP() == ops.Insert || msg.OP() == ops.Update {
			b := new(bytes.Buffer)
			json.NewEncoder(b).Encode(msg.Data())
			amqpMsg := amqp.Publishing{
				DeliveryMode: w.DeliveryMode,
				Timestamp:    time.Unix(msg.Timestamp(), 0),
				ContentType:  "application/json",
				Body:         b.Bytes(),
			}
			if w.KeyInField {
				err := s.(*Session).channel.Publish(msg.Namespace(), 
								    msg.Data().Get(w.RoutingKey).(string), 
								    false, 
								    false,
								    amqpMsg)
				s.(*Session).channel.Close()
			  	return msg, err
			} 
			err := s.(*Session).channel.Publish(msg.Namespace(), 
						            w.RoutingKey, 
							    false, 
							    false, 
						            amqpMsg)
			s.(*Session).channel.Close()
			return msg, err
			
		}
		s.(*Session).channel.Close()
		return msg, nil
	}
}
