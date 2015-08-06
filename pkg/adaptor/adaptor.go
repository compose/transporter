package adaptor

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/compose/transporter/pkg/pipe"
)

// ErrAdaptor gives the details of the failed adaptor
type ErrAdaptor struct {
	name string
}

func (a ErrAdaptor) Error() string {
	return fmt.Sprintf("adaptor '%s' not found in registry", a.name)
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
func Createadaptor(kind, path string, extra Config, p *pipe.Pipe) (adaptor StopStartListener, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("cannot create node: %v", r)
		}
	}()

	regentry, ok := Adaptors[kind]
	if !ok {
		return nil, ErrAdaptor{kind}
	}

	args := []reflect.Value{
		reflect.ValueOf(p),
		reflect.ValueOf(path),
		reflect.ValueOf(extra),
	}

	result := reflect.ValueOf(regentry.Constructor).Call(args)

	val := result[0]
	inter := result[1].Interface()

	if inter != nil {
		return nil, fmt.Errorf("cannot create %s adaptor (%s). %v", kind, path, inter.(error))
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
		return "", "", fmt.Errorf("malformed namespace, expected a '.' deliminated string")
	}
	return fields[0], fields[1], nil
}

// compileNamespace split's on the first '.' and then compiles the second portion to use as the msg filter
func (c *Config) compileNamespace() (string, *regexp.Regexp, error) {
	field0, field1, err := c.splitNamespace()

	if err != nil {
		return "", nil, err
	}

	compiledNs, err := regexp.Compile(strings.Trim(field1, "/"))
	return field0, compiledNs, err
}

// dbConfig is a standard typed config struct to use for as general purpose config for most databases.
type dbConfig struct {
	URI       string `json:"uri" doc:"the uri to connect to, in the form mongo://user:password@host.com:8080/database"` // the database uri
	Namespace string `json:"namespace" doc:"mongo namespace to read/write"`                                             // namespace
	Debug     bool   `json:"debug" doc:"display debug information"`                                                     // debug mode
}
