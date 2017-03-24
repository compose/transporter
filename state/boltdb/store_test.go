package boltdb

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/state"
)

var (
	lotsOfStates = []state.State{
		state.State{
			Identifier: 0,
			Mode:       state.Copy,
			Namespace:  "test",
			Timestamp:  uint64(time.Now().Unix()),
		},
		state.State{
			Identifier: 1,
			Mode:       state.Copy,
			Namespace:  "test",
			Timestamp:  uint64(time.Now().Unix()),
		},
		state.State{
			Identifier: 0,
			Mode:       state.Copy,
			Namespace:  "foo",
			Timestamp:  uint64(time.Now().Unix()),
		},
		state.State{
			Identifier: 1,
			Mode:       state.Sync,
			Namespace:  "foo",
			Timestamp:  uint64(time.Now().Unix()),
		},
		state.State{
			Identifier: bson.NewObjectId(),
			Mode:       state.Copy,
			Namespace:  "collection",
			Timestamp:  uint64(time.Now().Unix()),
		},
		state.State{
			Identifier: bson.NewObjectId(),
			Mode:       state.Copy,
			Namespace:  "collection",
			Timestamp:  uint64(time.Now().Unix()),
		},
	}
)

func TestStore_Apply(t *testing.T) {
	tmp, _ := ioutil.TempDir("", "store")
	defer os.RemoveAll(tmp)
	s, _ := New(tmp)
	type args struct {
		st state.State
	}
	tests := []struct {
		name    string
		s       *Store
		args    args
		wantErr bool
	}{
		{
			"int_Identifier",
			s,
			args{
				state.State{
					Identifier: 0,
					Mode:       state.Copy,
					Namespace:  "test",
					Timestamp:  uint64(time.Now().Unix()),
				},
			},
			false,
		},
		{
			"string_Identifier",
			s,
			args{
				state.State{
					Identifier: "abcdef",
					Mode:       state.Copy,
					Namespace:  "test",
					Timestamp:  uint64(time.Now().Unix()),
				},
			},
			false,
		},
		{
			"ObjectId_Identifier",
			s,
			args{
				state.State{
					Identifier: bson.NewObjectId(),
					Mode:       state.Copy,
					Namespace:  "test",
					Timestamp:  uint64(time.Now().Unix()),
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.s.Apply(tt.args.st); (err != nil) != tt.wantErr {
				t.Errorf("Store.Apply() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

var (
	randomObjectID = bson.NewObjectId()
)

func TestStore_All(t *testing.T) {
	tmp, _ := ioutil.TempDir("", "store")
	defer os.RemoveAll(tmp)
	s, _ := New(tmp)
	for _, st := range lotsOfStates {
		if err := s.Apply(st); err != nil {
			t.Fatalf("unexpected Store.Apply() error, %s", err)
		}
	}
	tests := []struct {
		name    string
		s       *Store
		want    []state.State
		wantErr bool
	}{
		{
			"multiple",
			s,
			[]state.State{lotsOfStates[5], lotsOfStates[3], lotsOfStates[1]},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.s.All()
			if (err != nil) != tt.wantErr {
				t.Errorf("Store.All() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Store.All() = %v, want %v", got, tt.want)
			}
		})
	}
}

func BenchmarkTransformOne(b *testing.B) {
	tmp, _ := ioutil.TempDir("", "store")
	defer os.RemoveAll(tmp)
	s, _ := New(tmp)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		loc := i % len(lotsOfStates)
		s.Apply(lotsOfStates[loc])
	}
}
