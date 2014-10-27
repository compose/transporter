package main

import (
	"encoding/json"
	"fmt"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/transporter"
	"github.com/nu7hatch/gouuid"
	"github.com/robertkrimen/otto"
)

// Node is a struct modelled after the transporter.Node struct, but
// more easily able to serialize to json for to use within the application.js
type Node struct {
	UUID     string
	Name     string         `json:"name"`
	Type     string         `json:"type"`
	Extra    adaptor.Config `json:"extra"`
	Children []*Node        `json:"children"`
	RootUUID string
}

// NewNode creates a node
func NewNode(name, kind string, extra adaptor.Config) (node Node, err error) {
	uuid, err := uuid.NewV4()
	if err != nil {
		return node, err
	}

	return Node{UUID: uuid.String(), Name: name, Type: kind, Extra: extra, RootUUID: uuid.String(), Children: make([]*Node, 0)}, nil
}

// CreateNode creates a node by marshalling an interface to json,
// and then unmarshalling into a struct.  Useful in the javascript builder
// to persist nodes in the js environment
func CreateNode(val interface{}) (Node, error) {
	t := Node{}
	ba, err := json.Marshal(val)

	if err != nil {
		return t, err
	}

	err = json.Unmarshal(ba, &t)
	return t, err
}

// Object turns this pipeline into an otto Object
func (n *Node) Object() (*otto.Object, error) {
	vm := otto.New()
	ba, err := json.Marshal(n)
	if err != nil {
		return nil, err
	}

	return vm.Object(fmt.Sprintf(`(%s)`, string(ba)))
}

// Add will add a node as a child of the current node
func (n *Node) Add(node *Node) {
	node.RootUUID = n.RootUUID
	n.Children = append(n.Children, node)
}

// CreateTransporterNode will turn this node into a transporter.Node.
// will recurse down the tree and transform each child
func (n *Node) CreateTransporterNode() *transporter.Node {
	self := transporter.NewNode(n.Name, n.Type, n.Extra)

	for _, child := range n.Children {
		self.Add(child.CreateTransporterNode())
	}

	return self
}
