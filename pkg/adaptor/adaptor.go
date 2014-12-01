package adaptor

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/compose/transporter/pkg/pipe"
)

var (
	// The node was not found in the map
	MissingNodeError = errors.New("adaptor not found in registry")

	// a registry of adaptor types and their constructors
	registry = map[string]interface{}{
		"mongo":         NewMongodb,
		"file":          NewFile,
		"elasticsearch": NewElasticsearch,
		"influx":        NewInfluxdb,
		"transformer":   NewTransformer,
	}
)

// Register registers an adaptor (database adaptor) for use with Transporter
// The second argument, fn, is a constructor that returns an instance of the
// given adaptor
func Register(name string, fn func(*pipe.Pipe, ExtraConfig) (StopStartListener, error)) {
	registry[name] = fn
}

// StopStartListener defines the interface that all database connectors and nodes must follow.
// Start() consumes data from the interface,
// Listen() listens on a pipe, processes data, and then emits it.
// Stop() shuts down the adaptor
type StopStartListener interface {
	Start() error
	Listen() error
	Stop() error
}

// Createadaptor instantiates an adaptor given the adaptor type and the ExtraConfig.
// Constructors are expected to be in the form
//   func NewWhatever(p *pipe.Pipe, extra ExtraConfig) (*Whatever, error) {}
// and are expected to confirm to the adaptor interface
func Createadaptor(kind string, extra ExtraConfig, p *pipe.Pipe) (adaptor StopStartListener, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("cannot create node: %v", r)
		}
	}()

	fn, ok := registry[kind]
	if !ok {
		return nil, MissingNodeError
	}

	args := []reflect.Value{
		reflect.ValueOf(p),
		reflect.ValueOf(extra),
	}

	result := reflect.ValueOf(fn).Call(args)

	val := result[0]
	inter := result[1].Interface()

	if inter != nil {
		return nil, inter.(error)
	}

	return val.Interface().(StopStartListener), err
}

// ExtraConfig is an alias to map[string]interface{} and helps us
// turn a fuzzy document into a conrete named struct
type ExtraConfig map[string]interface{}

// Construct will Marshal the ExtraConfig and then Unmarshal it into a
// named struct the generic map into a proper struct
func (c *ExtraConfig) Construct(conf interface{}) error {
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
func (c ExtraConfig) GetString(key string) string {
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
