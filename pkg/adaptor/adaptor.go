package adaptor

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

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
func Register(name string, fn func(*pipe.Pipe, Config) (StopStartListener, error)) {
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

// Createadaptor instantiates an adaptor given the adaptor type and the Config.
// Constructors are expected to be in the form
//   func NewWhatever(p *pipe.Pipe, extra Config) (*Whatever, error) {}
// and are expected to confirm to the adaptor interface
func Createadaptor(kind string, extra Config, p *pipe.Pipe) (adaptor StopStartListener, err error) {
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

// split a namespace into it's elements
// this covers a few standard cases, elasticsearch, mongo, rethink, but it's
// expected to be all inclusive.
func (c *Config) splitNamespace() (string, string, error) {
	fields := strings.SplitN(c.GetString("namespace"), ".", 2)

	if len(fields) != 2 {
		return "", "", fmt.Errorf("malformed namespace, expected a '.' deliminated string.")
	}
	return fields[0], fields[1], nil
}

func NewDBConfig(uri, namespace string, debug bool) DBConfig {
	return DBConfig{Uri: uri, Namespace: namespace, Debug: debug}
}

// InfluxdbConfig options
type DBConfig struct {
	Uri       string `json:"uri"`       // the database uri
	Namespace string `json:"namespace"` // namespace
	Debug     bool   `json:"debug"`     // debug mode
}
