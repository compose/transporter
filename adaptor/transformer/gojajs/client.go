package gojajs

import (
	"errors"
	"io/ioutil"

	"github.com/compose/transporter/client"

	"github.com/dop251/goja"
)

var (
	_ client.Client = &Client{}
	// ErrEmptyFilename will be returned when the profided filename is empty.
	ErrEmptyFilename = errors.New("no filename specified")
)

// JSFunc defines the structure a transformer function.
type JSFunc func(map[string]interface{}) *goja.Object

// Client represents a client to the underlying transformer function.
type Client struct {
	fn  string
	vm  *goja.Runtime
	jsf JSFunc
}

// ClientOptionFunc is a function that configures a Client.
// It is used in NewClient.
type ClientOptionFunc func(*Client) error

// NewClient creates a new client to work with Transformer functions.
//
// The caller can configure the new client by passing configuration options
// to the func.
//
// Example:
//
//   client, err := NewClient(
//     WithFilename("path/to/transformer.js"))
//
// If no URI is configured, it uses defaultURI by default.
//
// An error is also returned when some configuration option is invalid
func NewClient(options ...ClientOptionFunc) (*Client, error) {
	// Set up the client
	c := &Client{
		vm: nil,
	}

	// Run the options on it
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// WithFilename defines the path to the tranformer file.
func WithFilename(filename string) ClientOptionFunc {
	return func(c *Client) error {
		if filename == "" {
			return ErrEmptyFilename
		}

		ba, err := ioutil.ReadFile(filename)
		if err != nil {
			return err
		}

		c.fn = string(ba)
		return nil
	}
}

// WithFunction allows for passing a string version of the JS function.
func WithFunction(function string) ClientOptionFunc {
	return func(c *Client) error {
		c.fn = function
		return nil
	}
}

// Connect initializes the JS VM and tests the provided script.
func (c *Client) Connect() (client.Session, error) {
	if c.vm == nil {
		if err := c.initSession(); err != nil {
			return nil, err
		}
	}
	return &Session{c.vm, c.jsf}, nil
}

// initSession prepares the javascript vm and compiles the transformer script
func (c *Client) initSession() error {
	c.vm = goja.New()

	_, err := c.vm.RunString(c.fn)
	if err != nil {
		return err
	}
	var jsf JSFunc
	c.vm.ExportTo(c.vm.Get("transform"), &jsf)
	c.jsf = jsf
	return nil
}

// Session wraps the underlying otto.Otto vm for use by Writer.
type Session struct {
	vm *goja.Runtime
	fn JSFunc
}
