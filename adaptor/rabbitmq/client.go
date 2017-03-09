package rabbitmq

import (
	"crypto/tls"
	"crypto/x509"

	"github.com/compose/transporter/client"

	"github.com/streadway/amqp"
)

const (
	// DefaultURI is the default endpoint of RabbitMQ on the local machine.
	// Primarily used when initializing a new Client without a specific URI.
	DefaultURI = "amqp://guest:guest@localhost:5672/"
)

var (
	_ client.Client = &Client{}
)

// ClientOptionFunc is a function that configures a Client.
// It is used in NewClient.
type ClientOptionFunc func(*Client) error

// Client wraps the underlying connection to a RabbitMQ cluster.
type Client struct {
	uri       string
	tlsConfig *tls.Config
	conn      *amqp.Connection
}

// NewClient creates a new client to work with RabbitMQ.
//
// The caller can configure the new client by passing configuration options
// to the func.
//
// Example:
//
//   client, err := NewClient(
//     WithURI("mongodb://localhost:27017"))
//
// If no URI is configured, it uses DefaultURI.
//
// An error is also returned when a configuration option is invalid
func NewClient(options ...ClientOptionFunc) (*Client, error) {
	// Set up the client
	c := &Client{
		uri:       DefaultURI,
		tlsConfig: nil,
	}

	// Run the options on it
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// WithURI defines the full connection string for the RabbitMQ connection
func WithURI(uri string) ClientOptionFunc {
	return func(c *Client) error {
		if _, err := amqp.ParseURI(uri); err != nil {
			return client.InvalidURIError{URI: uri, Err: err.Error()}
		}
		c.uri = uri
		return nil
	}
}

// WithSSL configures the database connection to connect via TLS.
func WithSSL(ssl bool) ClientOptionFunc {
	return func(c *Client) error {
		if ssl {
			tlsConfig := &tls.Config{InsecureSkipVerify: true}
			tlsConfig.RootCAs = x509.NewCertPool()
			c.tlsConfig = tlsConfig
		}
		return nil
	}
}

// WithCACerts configures the RootCAs for the underlying TLS connection
func WithCACerts(certs []string) ClientOptionFunc {
	return func(c *Client) error {
		if len(certs) > 0 {
			roots := x509.NewCertPool()
			for _, cert := range certs {
				if ok := roots.AppendCertsFromPEM([]byte(cert)); !ok {
					return client.ErrInvalidCert
				}
			}
			if c.tlsConfig != nil {
				c.tlsConfig.RootCAs = roots
			} else {
				c.tlsConfig = &tls.Config{RootCAs: roots}
			}
			c.tlsConfig.InsecureSkipVerify = false
		}
		return nil
	}
}

// Connect satisfies the client.Client interface.
func (c *Client) Connect() (client.Session, error) {
	if c.conn == nil {
		if err := c.initConnection(); err != nil {
			return nil, err
		}
	}
	ch, err := c.conn.Channel()
	return &Session{c.conn, ch}, err
}

func (c *Client) initConnection() error {
	if c.tlsConfig != nil {
		conn, err := amqp.DialTLS(c.uri, c.tlsConfig)
		if err != nil {
			return client.ConnectError{Reason: err.Error()}
		}
		c.conn = conn
		return nil
	}
	conn, err := amqp.Dial(c.uri)
	if err != nil {
		return client.ConnectError{Reason: err.Error()}
	}
	c.conn = conn
	return nil
}

// Close implements necessary calls to cleanup the underlying connection.
func (c *Client) Close() {
	c.conn.Close()
}
