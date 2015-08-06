package adaptor

import (
	"fmt"
	"reflect"

	"github.com/compose/transporter/pkg/pipe"
)

var (
	// Adaptors is a registry of adaptor types, constructors and configs
	Adaptors = make(Registry)
)

func init() {
	Register("mongo", "a mongodb adaptor that functions as both a source and a sink", NewMongodb, MongodbConfig{})
	Register("file", "an adaptor that reads / writes files", NewFile, FileConfig{})
	Register("elasticsearch", "an elasticsearch sink adaptor", NewElasticsearch, dbConfig{})
	// Register("influx", "an InfluxDB sink adaptor", NewInfluxdb, dbConfig{})
	Register("transformer", "an adaptor that transforms documents using a javascript function", NewTransformer, TransformerConfig{})
	Register("rethinkdb", "a rethinkdb sink adaptor", NewRethinkdb, rethinkDbConfig{})
}

// Register registers an adaptor (database adaptor) for use with Transporter
// The second argument, fn, is a constructor that returns an instance of the
// given adaptor, config is an instance of the adaptor's config struct
func Register(name, desc string, fn func(*pipe.Pipe, string, Config) (StopStartListener, error), config interface{}) {
	Adaptors[name] = RegistryEntry{
		Name:        name,
		Description: desc,
		Constructor: fn,
		Config:      config,
	}
}

// Registry maps the adaptor's name to the RegistryEntry
type Registry map[string]RegistryEntry

// RegistryEntry stores the adaptor constructor and configuration struct
type RegistryEntry struct {
	Name        string
	Description string
	Constructor func(*pipe.Pipe, string, Config) (StopStartListener, error)
	Config      interface{}
}

// About inspects the  RegistryEntry's Config object, and uses
// each field's tags as a docstring
func (r *RegistryEntry) About() string {
	doc := fmt.Sprintf("%s %s\n\n", r.Name, r.Description)
	t := reflect.TypeOf(r.Config)
	doc += fmt.Sprintf("%-15s %-10s %s\n", "name", "type", "description")
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		doc += fmt.Sprintf("%-15s %-10s %s\n", f.Tag.Get("json"), f.Type, f.Tag.Get("doc"))
	}

	return doc
}
