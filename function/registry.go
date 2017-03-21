package function

import (
	"encoding/json"
	"fmt"
)

// ErrNotFound gives the details of the failed function
type ErrNotFound struct {
	Name string
}

func (a ErrNotFound) Error() string {
	return fmt.Sprintf("function '%s' not found in registry", a.Name)
}

// Creator defines the init structure for a Function.
type Creator func() Function

var functions = map[string]Creator{}

// Add should be called in init func of an implementing Function.
func Add(name string, creator Creator) {
	functions[name] = creator
}

// GetFunction looks up a function by name and then init's it with the provided map.
// returns ErrNotFound if the provided name was not registered.
func GetFunction(name string, conf map[string]interface{}) (Function, error) {
	creator, ok := functions[name]
	if ok {
		a := creator()
		b, err := json.Marshal(conf)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(b, a)
		if err != nil {
			return nil, err
		}
		return a, nil
	}
	return nil, ErrNotFound{name}
}

// RegisteredFunctions returns a slice of the names of every function registered.
func RegisteredFunctions() []string {
	all := make([]string, 0)
	for i := range functions {
		all = append(all, i)
	}
	return all
}
