package node

import (
	"errors"
	"reflect"
)

var NoImplError = errors.New("Module not found")

/*
 * our node implementation interface
 */
type NodeImpl interface {
	Start(Pipe) error
	Stop() error
}

var (
	Registry = map[string]interface{}{
		"mongo": NewMongoImpl,
		"file":  NewFileImpl,
	}
)

func NewImpl(n *Node) (NodeImpl, error) {
	var (
		err error
	)

	fn, ok := Registry[n.Type]
	if !ok {
		return nil, fmt.Errorf("Node type '%s' is not defined", n.Type)
	}

	args := []reflect.Value{
		reflect.ValueOf(n.Role),
		reflect.ValueOf(n.Name),
		reflect.ValueOf(n.Uri),
		reflect.ValueOf(n.Namespace),
	}

	result := reflect.ValueOf(fn).Call(args)
	impl := result[0]
	inter := result[1].Interface()

	if inter != nil {
		return nil, inter.(error)
	}

	if err != nil {
		return nil, err
	}

	switch m := impl.Interface().(type) {
	case *MongoImpl:
		return m, nil
	case *FileImpl:
		return m, nil
	}

	return nil, NoImplError
}
