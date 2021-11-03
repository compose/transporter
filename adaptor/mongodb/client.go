package mongodb

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"time"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	// DefaultURI is the default endpoint of MongoDB on the local machine.
	// Primarily used when initializing a new Client without a specific URI.
	DefaultURI = "mongodb://127.0.0.1:27017/test"

	// DefaultSessionTimeout is the default timeout after which the
	// session times out when unable to connect to the provided URI.
	DefaultSessionTimeout = 10 * time.Second

	// DefaultReadPreference when connecting to a mongo replica set.
	DefaultReadPreference = mgo.Primary
)

var (
	// DefaultSafety is the default saftey mode used for the underlying session.
	// These default settings are only good for local use as it makes not guarantees for writes.
	DefaultSafety = mgo.Safe{}

	_ client.Client = &Client{}
	_ client.Closer = &Client{}
)

// OplogAccessError wraps the underlying error when access to the oplog fails.
type OplogAccessError struct {
	reason string
}

func (e OplogAccessError) Error() string {
	return fmt.Sprintf("oplog access failed, %s", e.reason)
}

// InvalidReadPreferenceError represents the error when an incorrect mongo read preference has been set.
type InvalidReadPreferenceError struct {
	ReadPreference string
}

func (e InvalidReadPreferenceError) Error() string {
	return fmt.Sprintf("Invalid Read Preference, %s", e.ReadPreference)
}

// ClientOptionFunc is a function that configures a Client.
// It is used in NewClient.
type ClientOptionFunc func(*Client) error

// Client represents a client to the underlying MongoDB source.
type Client struct {
	uri string

	safety         mgo.Safe
	tlsConfig      *tls.Config
	sessionTimeout time.Duration
	tail           bool
	readPreference mgo.Mode

	mgoSession *mgo.Session
}

// NewClient creates a new client to work with MongoDB.
//
// The caller can configure the new client by passing configuration options
// to the func.
//
// Example:
//
//   client, err := NewClient(
//     WithURI("mongodb://localhost:27017"),
//     WithTimeout("30s"))
//
// If no URI is configured, it uses defaultURI by default.
//
// An error is also returned when some configuration option is invalid
func NewClient(options ...ClientOptionFunc) (*Client, error) {
	// Set up the client
	c := &Client{
		uri:            DefaultURI,
		sessionTimeout: DefaultSessionTimeout,
		safety:         DefaultSafety,
		tlsConfig:      nil,
		tail:           false,
		readPreference: DefaultReadPreference,
	}

	// Run the options on it
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// WithURI defines the full connection string of the MongoDB database.
func WithURI(uri string) ClientOptionFunc {
	return func(c *Client) error {
		_, err := mgo.ParseURL(uri)
		if err != nil {
			return client.InvalidURIError{URI: uri, Err: err.Error()}
		}
		c.uri = uri
		return nil
	}
}

// WithTimeout overrides the DefaultSessionTimeout and should be parseable by time.ParseDuration
func WithTimeout(timeout string) ClientOptionFunc {
	return func(c *Client) error {
		if timeout == "" {
			c.sessionTimeout = DefaultSessionTimeout
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
				if _, err := os.Stat(cert); err != nil {
					return errors.New("Cert file not found")
				}

				c, err := ioutil.ReadFile(cert)
				if err != nil {
					return err
				}

				if ok := roots.AppendCertsFromPEM(c); !ok {
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

// WithWriteConcern configures the write concern option for the session (Default: 0).
func WithWriteConcern(wc int) ClientOptionFunc {
	return func(c *Client) error {
		if wc > 0 {
			c.safety.W = wc
		}
		return nil
	}
}

// WithFsync configures whether the server will wait for Fsync to complete before returning
// a response (Default: false).
func WithFsync(fsync bool) ClientOptionFunc {
	return func(c *Client) error {
		c.safety.FSync = fsync
		return nil
	}
}

// WithTail set the flag to tell the Client whether or not access to the oplog will be
// needed (Default: false).
func WithTail(tail bool) ClientOptionFunc {
	return func(c *Client) error {
		c.tail = tail
		return nil
	}
}

// WithReadPreference sets the MongoDB read preference based on the provided string.
func WithReadPreference(readPreference string) ClientOptionFunc {
	return func(c *Client) error {
		if readPreference == "" {
			c.readPreference = DefaultReadPreference
			return nil
		}
		switch strings.ToLower(readPreference) {
		case "primary":
			c.readPreference = mgo.Primary
		case "primarypreferred":
			c.readPreference = mgo.PrimaryPreferred
		case "secondary":
			c.readPreference = mgo.Secondary
		case "secondarypreferred":
			c.readPreference = mgo.SecondaryPreferred
		case "nearest":
			c.readPreference = mgo.Nearest
		default:
			return InvalidReadPreferenceError{ReadPreference: readPreference}
		}
		return nil
	}
}

// Connect tests the mongodb connection and initializes the mongo session
func (c *Client) Connect() (client.Session, error) {
	if c.mgoSession == nil {
		if err := c.initConnection(); err != nil {
			return nil, err
		}
	}
	return c.session(), nil
}

// Close satisfies the Closer interface and handles closing the initial mgo.Session.
func (c Client) Close() {
	if c.mgoSession != nil {
		c.mgoSession.Close()
	}
}

func (c *Client) initConnection() error {
	// we can ignore the error since all Client's will either use the DefaultURI or SetURI
	dialInfo, _ := mgo.ParseURL(c.uri)

	if c.tlsConfig != nil {
		dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			fmt.Println()
			fmt.Printf("%+v\n", c.tlsConfig)
			return tls.Dial("tcp", addr.String(), c.tlsConfig)
		}
	}

	dialInfo.Timeout = c.sessionTimeout

	mgoSession, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		return client.ConnectError{Reason: err.Error()}
	}

	// set some options on the session
	// mgo logger _may_ be a bit too noisy but it'll be good to have for diagnosis
	mgo.SetLogger(log.Base())
	mgoSession.EnsureSafe(&c.safety)
	mgoSession.SetBatch(1000)
	mgoSession.SetPrefetch(0.5)
	mgoSession.SetSocketTimeout(time.Hour)
	mgoSession.SetMode(c.readPreference, true)

	if c.tail {
		log.With("uri", c.uri).Infoln("testing oplog access")
		localColls, err := mgoSession.DB("local").CollectionNames()
		if err != nil {
			return OplogAccessError{"unable to list collections on local database"}
		}
		oplogFound := false
		for _, c := range localColls {
			if c == "oplog.rs" {
				oplogFound = true
				break
			}
		}
		if !oplogFound {
			return OplogAccessError{"database missing oplog.rs collection"}
		}
		if err := mgoSession.DB("local").C("oplog.rs").Find(bson.M{}).Limit(1).One(nil); err != nil {
			return OplogAccessError{"not authorized for oplog.rs collection"}
		}
		log.Infoln("oplog access good")
	}
	c.mgoSession = mgoSession
	return nil
}

// Session fulfills the client.Client interface by providing a copy of the main mgoSession
func (c *Client) session() client.Session {
	sess := c.mgoSession.Copy()
	return &Session{sess}
}
