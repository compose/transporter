package transporter

import (
	"testing"

	"github.com/compose/transporter/pkg/impl"
)

func TestNodeString(t *testing.T) {
	data := []struct {
		in  *Node
		out string
	}{
		{
			&Node{},
			" - Source:                                                                                                 ",
		},
		{
			NewNode("name", "mongodb", map[string]interface{}{"namespace": "ns", "uri": "uri"}),
			" - Source:         name                                     mongodb         ns                             uri",
		},
	}

	for _, v := range data {
		if v.in.String() != v.out {
			t.Errorf("\nexpected: '%s'\n     got: '%s'\n", v.out, v.in.String())
		}
	}
}

func TestValidate(t *testing.T) {
	data := []struct {
		in  *Node
		out bool
	}{
		{
			NewNode("first", "mongo", impl.ExtraConfig{}),
			false,
		},
		{
			NewNode("second", "mongo", impl.ExtraConfig{}).Add(NewNode("name", "mongo", impl.ExtraConfig{})),
			true,
		},
		{
			NewNode("third", "mongo", impl.ExtraConfig{}).Add(NewNode("name", "transformer", impl.ExtraConfig{})),
			false,
		},
		{
			NewNode("fourth", "mongo", impl.ExtraConfig{}).Add(NewNode("name", "transformer", impl.ExtraConfig{}).Add(NewNode("name", "mongo", impl.ExtraConfig{}))),
			true,
		},
	}

	for _, v := range data {
		if v.in.Validate() != v.out {
			t.Errorf("%s: expected: %t got: %t", v.in.Name, v.out, v.in.Validate())
		}
	}
}
