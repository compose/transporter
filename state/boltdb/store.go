package boltdb

import (
	"fmt"
	"path/filepath"
	"sort"

	"gopkg.in/mgo.v2/bson"

	"github.com/boltdb/bolt"
	"github.com/compose/transporter/state"
)

const (
	storeName     = "transporter_state.db"
	defaultBucket = "message_states"
)

var (
	_ state.Store = &Store{}
)

type Store struct {
	db *bolt.DB
}

func New(path string) (*Store, error) {
	db, err := bolt.Open(filepath.Join(path, storeName), 0600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(defaultBucket))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	return &Store{db}, err
}

func (s *Store) Apply(st state.State) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(defaultBucket))
		d, err := bson.Marshal(st)
		if err != nil {
			return err
		}
		return b.Put([]byte(st.Namespace), d)
	})
}

func (s *Store) All() ([]state.State, error) {
	states := make([]state.State, 0)
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(defaultBucket))

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var st state.State
			if err := bson.Unmarshal(v, &st); err != nil {
				return err
			}
			states = append(states, st)
		}

		return nil
	})
	sort.Slice(states, func(i, j int) bool { return states[i].Namespace < states[j].Namespace })
	return states, err
}
