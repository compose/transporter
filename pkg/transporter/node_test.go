package transporter

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/pipe"
)

func TestConfigNodeString(t *testing.T) {
	data := []struct {
		in  ConfigNode
		out string
	}{
		{
			ConfigNode{},
			"                                     no namespace set               no uri set",
		},
		{
			ConfigNode{Name: "name", Type: "mongodb", Extra: map[string]interface{}{"namespace": "ns", "uri": "uri"}},
			"name                 mongodb         ns                             uri",
		},
	}

	for _, v := range data {
		if v.in.String() != v.out {
			t.Errorf("expected %s, got %s", v.out, v.in.String())
		}
	}
}

var anError = errors.New("this is an error")

type Impl struct {
	value string
}

func NewImpl(p pipe.Pipe, extra map[string]interface{}) (*Impl, error) {
	val, ok := extra["value"]
	if !ok {
		return nil, anError
	}
	return &Impl{value: val.(string)}, nil
}

func TestConfigNodeCallCreator(t *testing.T) {
	p := pipe.NewSourcePipe("name", 1*time.Second)
	data := []struct {
		in  ConfigNode
		out *Impl
		err error
	}{
		{
			ConfigNode{Extra: map[string]interface{}{"value": "rockettes"}},
			&Impl{value: "rockettes"},
			nil,
		},
		{
			ConfigNode{Extra: map[string]interface{}{"blah": "rockettes"}},
			&Impl{},
			anError,
		},
	}
	for _, v := range data {
		val, err := v.in.callCreator(p, NewImpl)

		if err != v.err {
			t.Errorf("expected error %v, got %v", v.err, err)
			t.FailNow()
		}
		if reflect.DeepEqual(v.out, val) {
			t.Errorf("expected %v, got %v", v.out, val)
		}
	}

}
