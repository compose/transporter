package node

import (
	"fmt"
	"io/ioutil"
)

// TODO: can we get rid of the NodeConfig?
// how to we turn a node into a concrete struct, that can actually do things

/*
 * A Node is an endpoint, Either a source, or a sink
 */
type Node struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Uri       string `json:"uri"`
	Namespace string `json:"namespace"`
}

func NewNode(config NodeConfig) *Node {
	return &Node{Name: config.Name, Type: config.Type, Uri: config.Uri}
}

func (n *Node) String() string {
	return fmt.Sprintf("%-20s %-15s %-30s %s", n.Name, n.Type, n.Namespace, n.Uri)
}

/*
 * NodeConfig describes the node in the Yaml config file
 */
type NodeConfig struct {
	Name string
	Type string
	Uri  string
}

func (n *NodeConfig) String() string {
	return fmt.Sprintf("%-20s %-15s %s", n.Name, n.Type, n.Uri)
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
