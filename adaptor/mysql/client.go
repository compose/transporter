package mysql
import (
	"database/sql"
	"net/url"
	"strings"
	//"fmt"

	"github.com/compose/transporter/client"

	_ "github.com/go-sql-driver/mysql" // import mysql driver
)

const (
	// DefaultURI is the default endpoint of MySQL on the local machine.
	// Primarily used when initializing a new Client without a specific URI.
	// Supposedly we should use a socket and not tcp if localhost, but that might be
	// more confusing for others when it comes to altering it?
	// https://github.com/go-sql-driver/mysql#dsn-data-source-name
	DefaultURI = "root@tcp(localhost)/"
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
		db:  "test", // Temporary change from `mysql`? The default local instance I have has `test`
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
	var err error
	var dsn string

	if c.mysqlSession == nil {
		// Previously it said here "there's really no way for this to error...", but that sounds
		// like terrible advice when developing, especially, as it took me  ages to figure out I
		// was getting:
		//
		// > panic: invalid DSN: missing the slash separating the database name
		//
		// So let's do _something_
		// Let's strip prefix if it is there since we need a DSN
		dsn = strings.Replace(c.uri, "mysql://", "", 1)
		// Debug:
		// fmt.Println(dsn)
		c.mysqlSession, err = sql.Open("mysql", dsn)
		if err != nil {
			panic(err.Error()) // TODO: Maybe not panic?
		}
		// For MySQL we can't really parse the uri because of https://pkg.go.dev/net/url#Parse
		//
		// > Trying to parse a hostname and path without a scheme is invalid but may not
		// > necessarily return an error, due to parsing ambiguities
		//
		// and MySQL is using a DSN. But we can cheat and add in a prefix/scheme
		uri, _ := url.Parse("mysql://" + c.uri)
		if uri.Path != "" {
			c.db = uri.Path[1:]
		}
	}
	err = c.mysqlSession.Ping()
	return &Session{c.mysqlSession, c.db}, err
}
