package ottojs

import (
	"errors"
	"io/ioutil"

	"github.com/compose/transporter/client"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore" // enable underscore
)

var (
	_ client.Client = &Client{}
	// ErrEmptyFilename will be returned when the profided filename is empty.
	ErrEmptyFilename = errors.New("no filename specified")
)

// Client represents a client to the underlying transformer function.
type Client struct {
	fn string
	vm *otto.Otto
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
	return &Session{c.vm}, nil
}

// initSession prepares the javascript vm and compiles the transformer script
func (c *Client) initSession() error {
	c.vm = otto.New()

	// set up the vm environment, make `module = {}`
	if _, err := c.vm.Run(`module = {}`); err != nil {
		return err
	}

	// compile our script
	script, err := c.vm.Compile("", c.fn)
	if err != nil {
		return err
	}

	// run the script, ignore the output
	if _, err = c.vm.Run(script); err != nil {
		return err
	}
	return nil
}

// Session wraps the underlying otto.Otto vm for use by Writer.
type Session struct {
	vm *otto.Otto
}
