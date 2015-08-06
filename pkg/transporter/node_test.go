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
			NewNode("name", "mongodb", adaptor.Config{"uri": "uri", "namespace": "db.col", "debug": false}),
			" - Source:         name                                     mongodb         db.col                         uri",
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
			NewNode("first", "mongo", adaptor.Config{}),
			false,
		},
		{
			NewNode("second", "mongo", adaptor.Config{}).Add(NewNode("name", "mongo", adaptor.Config{})),
			true,
		},
		{
			NewNode("third", "mongo", adaptor.Config{}).Add(NewNode("name", "transformer", adaptor.Config{})),
			false,
		},
		{
			NewNode("fourth", "mongo", adaptor.Config{}).Add(NewNode("name", "transformer", adaptor.Config{}).Add(NewNode("name", "mongo", adaptor.Config{}))),
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
			NewNode("first", "mongo", adaptor.Config{}),
			"first",
		},
		{
			NewNode("first", "mongo", adaptor.Config{}).Add(NewNode("second", "mongo", adaptor.Config{})),
			"first/second",
		},
		{
			NewNode("first", "mongo", adaptor.Config{}).Add(NewNode("second", "transformer", adaptor.Config{}).Add(NewNode("third", "mongo", adaptor.Config{}))),
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
