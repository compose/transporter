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

func TestPath(t *testing.T) {
	data := []struct {
		in  *Node
		out string
	}{
		{
			NewNode("first", "mongo", impl.ExtraConfig{}),
			"first",
		},
		{
			NewNode("first", "mongo", impl.ExtraConfig{}).Add(NewNode("second", "mongo", impl.ExtraConfig{})),
			"first/second",
		},
		{
			NewNode("first", "mongo", impl.ExtraConfig{}).Add(NewNode("second", "transformer", impl.ExtraConfig{}).Add(NewNode("third", "mongo", impl.ExtraConfig{}))),
			"first/second/third",
		},
	}

	for _, v := range data {
		node := v.in
		var path string
		for {
			if len(node.Children) == 0 {
				path = node.Path()
				break
			}
			node = node.Children[0]
		}
		if path != v.out {
			t.Errorf("%s: expected: %s got: %s", node.Name, v.out, path)
		}
	}
}
