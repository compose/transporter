package transporter

import (
	"testing"

	"github.com/compose/transporter/pkg/adaptor"
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
			NewNode("first", "mongo", adaptor.ExtraConfig{}),
			false,
		},
		{
			NewNode("second", "mongo", adaptor.ExtraConfig{}).Add(NewNode("name", "mongo", adaptor.ExtraConfig{})),
			true,
		},
		{
			NewNode("third", "mongo", adaptor.ExtraConfig{}).Add(NewNode("name", "transformer", adaptor.ExtraConfig{})),
			false,
		},
		{
			NewNode("fourth", "mongo", adaptor.ExtraConfig{}).Add(NewNode("name", "transformer", adaptor.ExtraConfig{}).Add(NewNode("name", "mongo", adaptor.ExtraConfig{}))),
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
			NewNode("first", "mongo", adaptor.ExtraConfig{}),
			"first",
		},
		{
			NewNode("first", "mongo", adaptor.ExtraConfig{}).Add(NewNode("second", "mongo", adaptor.ExtraConfig{})),
			"first/second",
		},
		{
			NewNode("first", "mongo", adaptor.ExtraConfig{}).Add(NewNode("second", "transformer", adaptor.ExtraConfig{}).Add(NewNode("third", "mongo", adaptor.ExtraConfig{}))),
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
