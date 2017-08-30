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
	_ adaptor.Adaptor = &rabbitMQ{}
)

// RabbitMQ defines all configurable elements for connecting to and sending/receiving JSON.
type rabbitMQ struct {
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
			return &rabbitMQ{
				BaseConfig:   adaptor.BaseConfig{URI: DefaultURI},
				RoutingKey:   DefaultRoutingKey,
				DeliveryMode: DefaultDeliveryMode,
				APIPort:      DefaultAPIPort,
			}
		},
	)
}

func (r *rabbitMQ) Client() (client.Client, error) {
	return NewClient(WithURI(r.URI),
		WithSSL(r.SSL),
		WithCACerts(r.CACerts))
}

func (r *rabbitMQ) Reader() (client.Reader, error) {
	return &Reader{r.URI, r.APIPort}, nil
}

func (r *rabbitMQ) Writer(done chan struct{}, wg *sync.WaitGroup) (client.Writer, error) {
	return &Writer{r.DeliveryMode, r.RoutingKey, r.KeyInField}, nil
}

func (r *rabbitMQ) Description() string {
	return description
}

func (r *rabbitMQ) SampleConfig() string {
	return sampleConfig
}
