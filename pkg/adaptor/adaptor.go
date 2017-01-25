package adaptor

import (
	"encoding/json"
	"fmt"
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

// Adaptor defines the interface that all database connectors and nodes must follow.
// Start() consumes data from the interface,
// Listen() listens on a pipe, processes data, and then emits it.
// Stop() shuts down the adaptor
type Adaptor interface {
	Start() error
	Listen() error
	Stop() error
}

// Connectable defines the interface that adapters should follow to have their connections set
// on load
// Connect() allows the adaptor an opportunity to setup connections prior to Start()
type Connectable interface {
	Connect() error
}

// Describable defines the interface that all database connectors and nodes must follow in order to support
// the help functions.
// SampleConfig() returns an example YAML structure to configure the adaptor
// Description() provides contextual information for what the adaptor is for
type Describable interface {
	SampleConfig() string
	Description() string
}

// CreateAdaptor instantiates an adaptor given the adaptor type and the Config.
// An Adaptor is expected to add themselves to the Adaptors map in the init() func
//   func init() {
//     adaptors.Add("TYPE", func(p *pipe.Pipe, path string, extra adaptors.Config) (adaptors.StopStartListener, error) {
//     })
//   }
// and are expected to confirm to the Adaptor interface
func CreateAdaptor(kind, path string, extra Config, p *pipe.Pipe) (adaptor Adaptor, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("cannot create node [%s]: %v", kind, r)
		}
	}()

	creator, ok := Adaptors[kind]
	if !ok {
		return nil, ErrAdaptor{kind}
	}

	adaptor, err = creator(p, path, extra)
	if err != nil {
		return nil, err
	}
	if c, ok := adaptor.(Connectable); ok {
		if err := c.Connect(); err != nil {
			return nil, err
		}
	}

	return adaptor, nil
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

// CompileNamespace split's on the first '.' and then compiles the second portion to use as the msg filter
func (c *Config) CompileNamespace() (string, *regexp.Regexp, error) {
	field0, field1, err := c.splitNamespace()

	if err != nil {
		return "", nil, err
	}

	compiledNs, err := regexp.Compile(strings.Trim(field1, "/"))
	return field0, compiledNs, err
}

// DbConfig is a standard typed config struct to use for as general purpose config for most databases.
type DbConfig struct {
	URI       string `json:"uri" doc:"the uri to connect to, in the form mongo://user:password@host.com:8080/database"` // the database uri
	Namespace string `json:"namespace" doc:"mongo namespace to read/write"`                                             // namespace
	Debug     bool   `json:"debug" doc:"display debug information"`                                                     // debug mode
}
