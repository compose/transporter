package mem

import (
	"errors"
	"sort"

	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/state"
)

var (
	_                 state.Store = &Store{}
	ErrEmptyNamespace             = errors.New("Namespace may not be empty")
)

type Store struct {
	stateMap map[string][]byte
}

func New() *Store {
	return &Store{
		stateMap: make(map[string][]byte),
	}
}

func (s *Store) Apply(st state.State) error {
	if st.Namespace == "" {
		return ErrEmptyNamespace
	}
	if current, ok := s.stateMap[st.Namespace]; ok {
		var existing state.State
		if err := bson.Unmarshal(current, &existing); err != nil {
			return err
		}
		if existing.MsgID > st.MsgID {
			return nil
		}
	}
	d, err := bson.Marshal(st)
	if err != nil {
		return err
	}
	s.stateMap[st.Namespace] = d
	return nil
}

func (s *Store) All() ([]state.State, error) {
	states := make([]state.State, len(s.stateMap))
	var index int
	for _, v := range s.stateMap {
		var st state.State
		if err := bson.Unmarshal(v, &st); err != nil {
			return nil, err
		}
		states[index] = st
		index++
	}
	sort.Slice(states, func(i, j int) bool { return states[i].Namespace < states[j].Namespace })
	return states, nil
}
