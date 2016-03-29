package adaptor

import "github.com/compose/transporter/pkg/pipe"

// Creator defines the init structure for an adaptor
type Creator func(*pipe.Pipe, string, Config) (Adaptor, error)

// adaptors stores a map of adaptors by name
var adaptors = map[string]Creator{}

// Add should be called in init func of adaptor
func Add(name string, creator Creator) {
	adaptors[name] = creator
}
