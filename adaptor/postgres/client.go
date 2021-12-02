package postgres

import (
	"database/sql"
	"net/url"

	"github.com/compose/transporter/client"

	_ "github.com/lib/pq" // import pq driver
)

const (
	// DefaultURI is the default endpoint of Postgres on the local machine.
	// Primarily used when initializing a new Client without a specific URI.
	DefaultURI = "postgres://postgres@transporter-db:5432?sslmode=disable"
)

var (
	_ client.Client = &Client{}
)

// ClientOptionFunc is a function that configures a Client.
// It is used in NewClient.
type ClientOptionFunc func(*Client) error

// Client represents a client to the underlying File source.
type Client struct {
	uri       string
	db        string
	pqSession *sql.DB
}

// NewClient creates a default file client
func NewClient(options ...ClientOptionFunc) (*Client, error) {
	// Set up the client
	c := &Client{
		uri: DefaultURI,
		db:  "postgres",
	}

	// Run the options on it
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// WithURI defines the full connection string for the Postgres connection
func WithURI(uri string) ClientOptionFunc {
	return func(c *Client) error {
		_, err := url.Parse(uri)
		c.uri = uri
		return err
	}
}

// Close implements necessary calls to cleanup the underlying *sql.DB
func (c *Client) Close() {
	if c.pqSession != nil {
		c.pqSession.Close()
	}
}

// Connect initializes the Postgres connection
func (c *Client) Connect() (client.Session, error) {
	if c.pqSession == nil {
		// there's really no way for this to error because we know the driver we're passing is
		// available.
		c.pqSession, _ = sql.Open("postgres", c.uri)
		uri, _ := url.Parse(c.uri)
		if uri.Path != "" {
			c.db = uri.Path[1:]
		}
	}
	err := c.pqSession.Ping()
	return &Session{c.pqSession, c.db}, err
}
