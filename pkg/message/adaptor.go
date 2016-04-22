package message

import (
	"fmt"
	"sync"

	"github.com/compose/transporter/pkg/message/ops"
)

type Adaptor interface {
	Name() string
	From(op ops.Op, namespace string, data interface{}) Msg
}

type Insertable interface {
	Insert(Msg) error
}

type Deletable interface {
	Delete(Msg) error
}

type Updatable interface {
	Update(Msg) error
}

type Commandable interface {
	Command(Msg) error
}

var (
	adaptorsMu sync.RWMutex
	adaptors   = make(map[string]Adaptor)
)

func Register(name string, adaptor Adaptor) {
	adaptorsMu.Lock()
	defer adaptorsMu.Unlock()
	if adaptor == nil {
		panic("adaptors: Register adaptor is nil")
	}
	if _, dup := adaptors[name]; dup {
		panic("adaptors: Register called twice for adaptor " + name)
	}
	adaptors[name] = adaptor
}

func MustUseAdaptor(name string) Adaptor {
	a, err := UseAdaptor(name)
	if err != nil {
		panic(err)
	}
	return a
}

func UseAdaptor(name string) (Adaptor, error) {
	adaptorsMu.RLock()
	defer adaptorsMu.RUnlock()
	a, dup := adaptors[name]
	if !dup {
		return nil, fmt.Errorf("no adaptor found for %s", name)
	}
	return a, nil
}

func Exec(a Adaptor, m Msg) (Msg, error) {
	switch m.OP() {
	case ops.Insert:
		if i, ok := a.(Insertable); ok {
			return m, i.Insert(m)
		}
		return m, fmt.Errorf("adaptor %s is not Insertable", a.Name())
	case ops.Update:
		if i, ok := a.(Updatable); ok {
			return m, i.Update(m)
		}
		return m, fmt.Errorf("adaptor %s is not Updatable", a.Name())
	case ops.Delete:
		if i, ok := a.(Deletable); ok {
			return m, i.Delete(m)
		}
		return m, fmt.Errorf("adaptor %s is not Deletable", a.Name())
	case ops.Command:
		if i, ok := a.(Commandable); ok {
			return m, i.Command(m)
		}
		return m, fmt.Errorf("adaptor %s is not Commandable", a.Name())
	case ops.Noop:
		return m, nil
	}
	return m, fmt.Errorf("no adaptor support for op: %s", m.OP())
}
