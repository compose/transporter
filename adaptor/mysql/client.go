package mysql
import (
	"database/sql"
	"net/url"

	"github.com/compose/transporter/client"

	_ "github.com/go-sql-driver/mysql" // import mysql driver
)

const (
	// DefaultURI is the default endpoint of MySQL on the local machine.
	// Primarily used when initializing a new Client without a specific URI.
	DefaultURI = "mysql://127.0.0.1:3306"
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
	mysqlSession *sql.DB
}

// NewClient creates a default file client
func NewClient(options ...ClientOptionFunc) (*Client, error) {
	// Set up the client
	c := &Client{
		uri: DefaultURI,
		db:  "mysql", // Or "compose"?
	}

	// Run the options on it
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// WithURI defines the full connection string for the MySQL connection
func WithURI(uri string) ClientOptionFunc {
	return func(c *Client) error {
		_, err := url.Parse(uri)
		c.uri = uri
		return err
	}
}

// Close implements necessary calls to cleanup the underlying *sql.DB
func (c *Client) Close() {
	if c.mysqlSession != nil {
		c.mysqlSession.Close()
	}
}

// Connect initializes the MySQL connection
func (c *Client) Connect() (client.Session, error) {
	if c.mysqlSession == nil {
		// there's really no way for this to error because we know the driver we're passing is
		// available.
		c.mysqlSession, _ = sql.Open("mysql", c.uri)
		uri, _ := url.Parse(c.uri)
		if uri.Path != "" {
			c.db = uri.Path[1:]
		}
	}
	err := c.mysqlSession.Ping()
	return &Session{c.mysqlSession, c.db}, err
}
