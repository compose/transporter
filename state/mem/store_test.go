package mem

import (
	"reflect"
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/state"
)

var (
	multiStates = New()

	lotsOfStates = []state.State{
		state.State{
			MsgID:      0,
			Identifier: 0,
			Mode:       state.Copy,
			Namespace:  "test",
			Timestamp:  uint64(time.Now().Unix()),
		},
		state.State{
			MsgID:      1,
			Identifier: 1,
			Mode:       state.Copy,
			Namespace:  "test",
			Timestamp:  uint64(time.Now().Unix()),
		},
		state.State{
			MsgID:      0,
			Identifier: 0,
			Mode:       state.Copy,
			Namespace:  "test",
			Timestamp:  uint64(time.Now().Unix()),
		},
		state.State{
			MsgID:      0,
			Identifier: 0,
			Mode:       state.Copy,
			Namespace:  "foo",
			Timestamp:  uint64(time.Now().Unix()),
		},
		state.State{
			MsgID:      1,
			Identifier: 1,
			Mode:       state.Sync,
			Namespace:  "foo",
			Timestamp:  uint64(time.Now().Unix()),
		},
	}
)

func init() {
	for _, st := range lotsOfStates {
		multiStates.Apply(st)
	}
}

func stateInBytes(st state.State) []byte {
	d, _ := bson.Marshal(st)
	return d
}

func TestStore_Apply(t *testing.T) {
	type args struct {
		in0 state.State
	}
	tests := []struct {
		name    string
		s       *Store
		args    args
		wantErr error
	}{
		{"missing Namespace", New(), args{in0: state.State{}}, ErrEmptyNamespace},
		{"blank Namespace", New(), args{in0: state.State{Namespace: ""}}, ErrEmptyNamespace},
		{"success", New(), args{in0: lotsOfStates[0]}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.s.Apply(tt.args.in0); err != tt.wantErr {
				t.Errorf("Store.Apply() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStore_All(t *testing.T) {
	tests := []struct {
		name    string
		s       *Store
		want    []state.State
		wantErr bool
	}{
		{
			"no state",
			&Store{stateMap: map[string][]byte{}},
			[]state.State{},
			false,
		},
		{
			"single state",
			&Store{
				stateMap: map[string][]byte{
					"test": stateInBytes(lotsOfStates[0]),
				},
			},
			[]state.State{lotsOfStates[0]},
			false,
		},
		{
			"multi state",
			multiStates,
			[]state.State{lotsOfStates[4], lotsOfStates[1]},
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
				t.Errorf("Store.All() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func BenchmarkTransformOne(b *testing.B) {
	s := New()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		loc := i % len(lotsOfStates)
		s.Apply(lotsOfStates[loc])
	}
}
