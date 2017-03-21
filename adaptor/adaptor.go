package adaptor

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/compose/transporter/client"
)

// ErrNamespaceMalformed represents the error to be returned when an invalid namespace is given.
var ErrNamespaceMalformed = errors.New("malformed namespace, expected a '.' deliminated string")

// ErrNotFound gives the details of the failed adaptor
type ErrNotFound struct {
	Name string
}

func (a ErrNotFound) Error() string {
	return fmt.Sprintf("adaptor '%s' not found in registry", a.Name)
}

// ErrFuncNotSupported should be used for adaptors that do not support a given function defined
// by the interface.
type ErrFuncNotSupported struct {
	Name string
	Func string
}

func (a ErrFuncNotSupported) Error() string {
	return fmt.Sprintf("'%s' does not support '%s' function", a.Name, a.Func)
}

// Adaptor defines the interface which provides functions to create client interfaces
type Adaptor interface {
	Client() (client.Client, error)
	Reader() (client.Reader, error)
	Writer(chan struct{}, *sync.WaitGroup) (client.Writer, error)
}

// Connectable defines the interface that adapters should follow to have their connections set
// on load
// Connect() allows the adaptor an opportunity to setup connections prior to Start()
type Connectable interface {
	Connect() error
}

// Describable defines the interface that all database connectors and nodes must follow in order to support
// the help functions.
// SampleConfig() returns an example JSON structure to configure the adaptor
// Description() provides contextual information for what the adaptor is for
type Describable interface {
	SampleConfig() string
	Description() string
}

// Config is an alias to map[string]interface{} and helps us
// turn a fuzzy document into a conrete named struct
type Config map[string]interface{}

// Construct will Marshal the Config and then Unmarshal it into a
// named struct the generic map into a proper struct
func (c *Config) Construct(conf interface{}) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	err = json.Unmarshal(b, conf)
	if err != nil {
		return err
	}
	return nil
}

// GetString returns value stored in the config under the given key, or
// an empty string if the key doesn't exist, or isn't a string value
func (c Config) GetString(key string) string {
	i, ok := c[key]
	if !ok {
		return ""
	}
	s, ok := i.(string)
	if !ok {
		return ""
	}
	return s
}

// BaseConfig is a standard typed config struct to use for as general purpose config for most databases.
type BaseConfig struct {
	URI     string `json:"uri"`
	Timeout string `json:"timeout"`
}
