package rabbitmq

import (
	"sync"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
)

const (
	sampleConfig = `{
  "uri": "${RABBITMQ_URI}",
  "routing_key": "",
  "key_in_field": false
  // "delivery_mode": 1, // non-persistent (1) or persistent (2)
  // "api_port": 15672,
  // "ssl": false,
  // "cacerts": ["/path/to/cert.pem"]
}`

	description = "an adaptor that handles publish/subscribe messaging with RabbitMQ"
)

var (
	_ adaptor.Adaptor = &RabbitMQ{}
)

// RabbitMQ defines all configurable elements for connecting to and sending/receiving JSON.
type RabbitMQ struct {
	adaptor.BaseConfig
	RoutingKey   string   `json:"routing_key"`
	KeyInField   bool     `json:"key_in_field"`
	DeliveryMode uint8    `json:"delivery_mode"`
	APIPort      int      `json:"api_port"`
	SSL          bool     `json:"ssl"`
	CACerts      []string `json:"cacerts"`
}

func init() {
	adaptor.Add(
		"rabbitmq",
		func() adaptor.Adaptor {
			return &RabbitMQ{
				BaseConfig:   adaptor.BaseConfig{URI: DefaultURI},
				RoutingKey:   DefaultRoutingKey,
				DeliveryMode: DefaultDeliveryMode,
				APIPort:      DefaultAPIPort,
			}
		},
	)
}

// Client creates an instance of Client to be used for connecting to RabbitMQ.
func (r *RabbitMQ) Client() (client.Client, error) {
	return NewClient(WithURI(r.URI),
		WithSSL(r.SSL),
		WithCACerts(r.CACerts))
}

// Reader instantiates a Reader for use with subscribing to one or more topics.
func (r *RabbitMQ) Reader() (client.Reader, error) {
	return &Reader{r.URI, r.APIPort}, nil
}

// Writer instantiates a Writer for use with publishing to one or more exchanges.
func (r *RabbitMQ) Writer(done chan struct{}, wg *sync.WaitGroup) (client.Writer, error) {
	return &Writer{r.DeliveryMode, r.RoutingKey, r.KeyInField}, nil
}

// Description for file adaptor
func (r *RabbitMQ) Description() string {
	return description
}

// SampleConfig for file adaptor
func (r *RabbitMQ) SampleConfig() string {
	return sampleConfig
}
