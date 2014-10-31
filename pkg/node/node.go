package node

import (
	"fmt"
	"io/ioutil"
)

/*
 * A Node is an endpoint, Either a source, or a sink
 */
type Node struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Uri       string `json:"uri"`
	Namespace string `json:"namespace"`
	NodeImpl  NodeImpl
}

func (n *Node) String() string {
	return fmt.Sprintf("%-20s %-15s %-30s %s", n.Name, n.Type, n.Namespace, n.Uri)
}

/*
 * Tie this to the actual implementation
 */
func (n *Node) Create() (err error) {
	fn, ok := Registry[n.Type]
	if !ok {
		return fmt.Errorf("Node type '%s' is not defined", n.Type)
	}
	n.NodeImpl, err = NewImpl(fn, n)
	return err
}

/*
 * Transformer
 */
type Transformer struct {
	Name string `json:"name"`
	Func string `json:"func"`
}

func NewTransformer() *Transformer {
	return &Transformer{}
}

func (t *Transformer) Load(filename string) error {
	ba, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	t.Name = filename
	t.Func = string(ba)
	return nil
}

func (t *Transformer) String() string {
	return fmt.Sprintf("%-20s %-15s", t.Name, "Transformer")
}
