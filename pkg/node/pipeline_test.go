package node

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestPipeline(t *testing.T) {
	data := []struct {
		in Pipeline
	}{
		{
			Pipeline{Source: &Node{Name: "nick"}, Sink: &Node{Name: "nick2"}, Transformers: []*Transformer{&Transformer{Func: "transformer1"}}},
		},
	}

	for _, v := range data {
		ba, err := json.Marshal(v.in)
		t.Logf("%+v", string(ba))
		if err != nil {
			t.Errorf("got error turning obj into bytearray, %v", err)
		}

		anon := map[string]interface{}{}
		err = json.Unmarshal(ba, &anon)
		if err != nil {
			t.Errorf("got error turning bytearray into anonymous map, %v", err)
		}

		obj, _ := InterfaceToPipeline(anon)
		if err != nil {
			t.Errorf("got error turning val into obj, %v", err)
		}

		if !reflect.DeepEqual(obj, v.in) {
			t.Errorf("Pipline toValue isn't an identity fn.  expected %+v, got %+v", v.in, obj)
		}
	}
}
