package rabbitmq

import "github.com/streadway/amqp"

// Session encapsulates an amqp.Connection and amqp.Channel for use by a Reader/Writer.
type Session struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}
