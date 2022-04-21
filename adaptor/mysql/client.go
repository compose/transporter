package mysql
import (
	"database/sql"
	"errors"
	"io/ioutil"
	"net/url"
	"os"
	"strings"
	//"fmt"

	"github.com/compose/transporter/client"

	//_ "github.com/go-sql-driver/mysql" // import mysql driver
	"github.com/go-mysql-org/go-mysql/driver" // full import of alternative mysql driver
)

const (
	// DefaultURI is the default endpoint of MySQL on the local machine.
	// Primarily used when initializing a new Client without a specific URI.
	DefaultURI = "mysql://root@localhost:3306?"
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
		db:  "test", // TODO: Temporary change from `mysql`? The default local
					 // instance I have has `test`, but that was before I
					 // switched to connecting as root
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
// Make this handle the different DSNs for these two?
// - https://github.com/go-sql-driver/mysql#dsn-data-source-name
// - https://github.com/go-mysql-org/go-mysql#driver
func WithURI(uri string) ClientOptionFunc {
	return func(c *Client) error {
		_, err := url.Parse(uri)
		c.uri = uri
		return err
	}
}

// WithCustomTLS configures the RootCAs for the underlying TLS connection
func WithCustomTLS(uri string, cert string, serverName string) ClientOptionFunc {
	return func(c *Client) error {
		if cert == "" {
			// Then there are no TLS options to configure
			return nil
		}
		if _, err := os.Stat(cert); err != nil {
			return errors.New("Cert file not found")
		}

		caPem, err := ioutil.ReadFile(cert)
		if err != nil {
			return err
		}

		// TODO: Make proper debug
		//fmt.Printf("Cert: %s", caPem)
		// Pass through to the driver
		// If serverName then don't do insecureSkipVerify
		insecureSkipVerify := true
		if serverName != "" {
			insecureSkipVerify = false
		}
		driver.SetCustomTLSConfig(uri, caPem, make([]byte, 0), make([]byte, 0), insecureSkipVerify, serverName)
		return nil
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
		// Also, let's strip prefix if it is there since we need a DSN
		dsn = strings.Replace(c.uri, "mysql://", "", 1)
		// TODO: Remove below that was for debugging/developing
		// fmt.Println(dsn)
		c.mysqlSession, err = sql.Open("mysql", dsn)
		if err != nil {
			panic(err.Error()) // TODO: Maybe not panic?
		}
		//fmt.Println(c.uri)
		uri, _ := url.Parse(c.uri)
		if uri.Path != "" {
			c.db = uri.Path[1:]
		}
	}
	// Replace Ping with an Exec that we need to run for imports anyway
	// TODO: Remove below rather than just have commented out
	//err = c.mysqlSession.Ping()
	// We need to disable Foreign Key Checks for imports
	// Ideally we don't want to send this _every_ time just once per session
	_, err = c.mysqlSession.Exec("SET FOREIGN_KEY_CHECKS=0;")
	return &Session{c.mysqlSession, c.db}, err
}
