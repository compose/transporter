package rethinkdb

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	version "github.com/hashicorp/go-version"

	r "gopkg.in/gorethink/gorethink.v3"
)

const (
	// DefaultURI is the default endpoint for RethinkDB on the local machine.
	// Primarily used when initializing a new Client without a specific URI.
	DefaultURI = "rethinkdb://127.0.0.1:28015/test"

	// DefaultTimeout is the default time.Duration used if one is not provided for options
	// that pertain to timeouts.
	DefaultTimeout = 10 * time.Second
)

var (
	_ client.Client = &Client{}
	_ client.Closer = &Client{}

	rethinkDbVersionMatcher = regexp.MustCompile(`\d+\.\d+(\.\d+)?`)
)

// Client creates and holds the session to RethinkDB
type Client struct {
	db, uri string

	sessionTimeout, writeTimeout, readTimeout time.Duration

	tlsConfig *tls.Config

	session *r.Session
}

// Session contains an instance of the rethink.Session for use by Readers/Writers
type Session struct {
	session *r.Session
}

// ClientOptionFunc is a function that configures a Client.
// It is used in NewClient.
type ClientOptionFunc func(*Client) error

// NewClient creates a new client to work with RethinkDB.
//
// The caller can configure the new client by passing configuration options
// to the func.
//
// Example:
//
//   client, err := NewClient(
//     WithURI("rethinkdb://localhost:28015"),
//     WithTimeout("30s"))
//
// If no URI is configured, it uses DefaultURI.
//
// An error is also returned when some configuration option is invalid
func NewClient(options ...ClientOptionFunc) (*Client, error) {
	// Set up the client
	c := &Client{
		uri:            DefaultURI,
		sessionTimeout: DefaultTimeout,
		tlsConfig:      nil,
	}

	// Run the options on it
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// WithURI defines the full connection string of the RethinkDB database.
func WithURI(uri string) ClientOptionFunc {
	return func(c *Client) error {
		_, err := url.Parse(c.uri)
		if err != nil {
			return client.InvalidURIError{URI: uri, Err: err.Error()}
		}
		c.uri = uri
		return nil
	}
}

// WithSessionTimeout overrides the DefaultTimeout and should be parseable by time.ParseDuration
func WithSessionTimeout(timeout string) ClientOptionFunc {
	return func(c *Client) error {
		if timeout == "" {
			c.sessionTimeout = DefaultTimeout
			return nil
		}

		t, err := time.ParseDuration(timeout)
		if err != nil {
			return client.InvalidTimeoutError{Timeout: timeout}
		}
		c.sessionTimeout = t
		return nil
	}
}

// WithWriteTimeout overrides the DefaultTimeout and should be parseable by time.ParseDuration
func WithWriteTimeout(timeout string) ClientOptionFunc {
	return func(c *Client) error {
		if timeout == "" {
			c.writeTimeout = DefaultTimeout
			return nil
		}

		t, err := time.ParseDuration(timeout)
		if err != nil {
			return client.InvalidTimeoutError{Timeout: timeout}
		}
		c.writeTimeout = t
		return nil
	}
}

// WithReadTimeout overrides the DefaultTimeout and should be parseable by time.ParseDuration
func WithReadTimeout(timeout string) ClientOptionFunc {
	return func(c *Client) error {
		if timeout == "" {
			c.readTimeout = DefaultTimeout
			return nil
		}

		t, err := time.ParseDuration(timeout)
		if err != nil {
			return client.InvalidTimeoutError{Timeout: timeout}
		}
		c.readTimeout = t
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
				if _, err := os.Stat(cert); err == nil {
					filepath.Abs(cert)
					c, err := ioutil.ReadFile(cert)
					if err != nil {
						return err
					}
					cert = string(c)
				}
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

// Connect wraps the underlying session to the RethinkDB database
func (c *Client) Connect() (client.Session, error) {
	if c.session == nil {
		if err := c.initConnection(); err != nil {
			return nil, err
		}
	}
	return &Session{c.session}, nil
}

// Close fulfills the Closer interface and takes care of cleaning up the rethink.Session
func (c *Client) Close() {
	if c.session != nil {
		c.session.Close(r.CloseOpts{NoReplyWait: false})
	}
}

func (c *Client) initConnection() error {
	uri, _ := url.Parse(c.uri)

	c.db = uri.Path[1:]
	opts := r.ConnectOpts{
		Addresses:    strings.Split(uri.Host, ","),
		Database:     c.db,
		Timeout:      c.sessionTimeout,
		WriteTimeout: c.writeTimeout,
		MaxIdle:      10,
		MaxOpen:      20,
		TLSConfig:    c.tlsConfig,
	}

	if uri.User != nil {
		if pwd, ok := uri.User.Password(); ok {
			opts.Username = uri.User.Username()
			opts.Password = pwd
		}
	}

	log.With("options", opts).Debugln("connection info")
	var err error
	c.session, err = r.Connect(opts)
	if err != nil {
		return client.ConnectError{Reason: err.Error()}
	}
	r.Log = log.Orig()
	return c.assertServerVersion()
}

func (c *Client) assertServerVersion() error {
	constraint, _ := version.NewConstraint(">= 2.0")

	cursor, err := r.DB("rethinkdb").Table("server_status").Run(c.session)
	if err != nil {
		return err
	}

	if cursor.IsNil() {
		return errors.New("could not determine the RethinkDB server version: no rows returned from the server_status table")
	}

	var serverStatus struct {
		Process struct {
			Version string `gorethink:"version"`
		} `gorethink:"process"`
	}
	cursor.Next(&serverStatus)

	if serverStatus.Process.Version == "" {
		return client.VersionError{
			URI: c.uri,
			V:   serverStatus.Process.Version,
			Err: "could not determine the RethinkDB server version: process.version key missing",
		}
	}

	versionString := rethinkDbVersionMatcher.FindString(strings.Split(serverStatus.Process.Version, " ")[1])
	if versionString == "" {
		return client.VersionError{
			URI: c.uri,
			V:   serverStatus.Process.Version,
			Err: "malformed version string",
		}
	}

	v, err := version.NewVersion(versionString)
	if err != nil {
		return client.VersionError{
			URI: c.uri,
			V:   serverStatus.Process.Version,
			Err: err.Error(),
		}
	}

	log.With("version", serverStatus.Process.Version).Debugln("rethinkdb server info")

	if !constraint.Check(v) {
		return fmt.Errorf("RethinkDB server version too old: expected %v, but was %v", constraint, v)
	}

	return nil
}
