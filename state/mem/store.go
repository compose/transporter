package mem

import (
	"errors"
	"sort"

	"github.com/compose/transporter/state"
)

var (
	_                 state.Store = &Store{}
	ErrEmptyNamespace             = errors.New("Namespace may not be empty")
)

type Store struct {
	stateMap map[string]state.State
}

func New() *Store {
	return &Store{
		stateMap: make(map[string]state.State),
	}
}
func (s *Store) Apply(st state.State) error {
	if st.Namespace == "" {
		return ErrEmptyNamespace
	}
	s.stateMap[st.Namespace] = st
	return nil
}

func (s *Store) All() ([]state.State, error) {
	states := make([]state.State, len(s.stateMap))
	var index int
	for _, v := range s.stateMap {
		states[index] = v
		index++
	}
	sort.Slice(states, func(i, j int) bool { return states[i].Namespace < states[j].Namespace })
	return states, nil
}
