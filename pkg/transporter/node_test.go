package transporter

import (
	"testing"
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
