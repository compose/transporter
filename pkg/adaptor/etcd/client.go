package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/compose/transporter/pkg/client"
	t_err "github.com/compose/transporter/pkg/error"
	"github.com/compose/transporter/pkg/log"

	eclient "github.com/coreos/etcd/client"
)

const (
	// DefaultRequestTimeout is the default timeout after which the
	// session times out when unable to connect to the provided URI.
	DefaultRequestTimeout = 5 * time.Second
)

var (
	_ client.Client = &Client{}

	// DefaultEndpoints is the default endpoints of etcd on the local machine.
	// Primarily used when initializing a new Client without a specific URI.
	DefaultEndpoints = []string{"http://127.0.0.1:2379"}
)

// Client represents a client to the underlying etcd cluster.
type Client struct {
	cfg eclient.Config
	e   eclient.Client
}

// ClientOptionFunc is a function that configures a Client.
// It is used in NewClient.
type ClientOptionFunc func(*Client) error

// NewClient creates a new client to work with etcd.
//
// The caller can configure the new client by passing configuration options
// to the func.
//
// Example:
//
//   client, err := NewClient(
//     WithURI("http://127.0.0.1:2379"),
//     WithTimeout("10s"))
//
// If no URI is configured, it uses DefaultEndpoints.
//
// An error is also returned when some configuration option is invalid
func NewClient(options ...ClientOptionFunc) (*Client, error) {
	// Set up the client
	c := &Client{
		cfg: eclient.Config{
			Endpoints:               DefaultEndpoints,
			HeaderTimeoutPerRequest: DefaultRequestTimeout,
			Transport:               eclient.DefaultTransport,
		},
	}

	// Run the options on it
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// WithURI defines all known etcd endpoints has a single string separated by a comma.
func WithURI(uri string) ClientOptionFunc {
	return func(c *Client) error {
		u, err := url.Parse(uri)
		if err != nil {
			return t_err.InvalidURIError{URI: uri, ErrDetail: err.Error()}
		}
		fmt.Printf("parsed uri: %+v\n", u)
		hostsAndPorts := strings.Split(u.Host, ",")
		urls := make([]string, len(hostsAndPorts))
		for i, hAndP := range hostsAndPorts {
			urls[i] = fmt.Sprintf("%s://%s", u.Scheme, hAndP)
		}
		c.cfg.Endpoints = urls
		if u.User != nil {
			if pwd, ok := u.User.Password(); ok {
				c.cfg.Username = u.User.Username()
				c.cfg.Password = pwd
			}
		}
		return nil
	}
}

// WithTimeout overrides the DefaultSessionTimeout and should be parseable by time.ParseDuration
func WithTimeout(timeout string) ClientOptionFunc {
	return func(c *Client) error {
		if timeout == "" {
			c.cfg.HeaderTimeoutPerRequest = DefaultRequestTimeout
			return nil
		}

		t, err := time.ParseDuration(timeout)
		if err != nil {
			return t_err.InvalidTimeoutError{Timeout: timeout}
		}
		c.cfg.HeaderTimeoutPerRequest = t
		return nil
	}
}

// Connect tests the etcd endpoints and initializes the session
func (c *Client) Connect() (client.Session, error) {
	if c.e == nil {
		var err error
		c.e, err = eclient.New(c.cfg)
		if err != nil {
			return nil, err
		}
		_, v, err := c.e.Do(context.Background(), &getVersion{Prefix: "/version"})
		if err != nil {
			return nil, t_err.ConnectError{Reason: "request to get version failed"}
		}
		var version etcdVersion
		json.NewDecoder(bytes.NewReader(v)).Decode(&version)
		log.With("server version", version.Server).With("cluster version", version.Cluster).Infoln("etcd info")
	}
	return &Session{Client: c.e}, nil
}

type getVersion struct {
	Prefix string
}

type etcdVersion struct {
	Server  string `json:"etcdserver"`
	Cluster string `json:"etcdcluster"`
}

func (g *getVersion) HTTPRequest(ep url.URL) *http.Request {
	ep.Path = g.Prefix
	req, _ := http.NewRequest("GET", ep.String(), nil)
	return req
}
