package node

import (
	"errors"
	"fmt"
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
	}
)

func NewImpl(fn interface{}, n *Node) (NodeImpl, error) {
	var (
		err error
		// errorType = reflect.TypeOf(make([]error, 1)).Elem()
	)

	args := []reflect.Value{
		reflect.ValueOf(n.Name),
		reflect.ValueOf(n.Type),
		reflect.ValueOf(n.Uri),
		reflect.ValueOf(n.Namespace),
	}
	result := reflect.ValueOf(fn).Call(args)
	impl := result[0]
	fmt.Printf("%T %v %T\n", result[1], result[1], result[1].Interface())

	// if result[1].Convert(errorType) == nil {
	// 	fmt.Println("nil!")
	// }
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
	}

	return nil, NoImplError
}
