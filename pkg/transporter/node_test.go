package transporter

import (
	"testing"
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
