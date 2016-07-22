package main

import (
	"encoding/json"
	"fmt"

	"git.compose.io/compose/transporter/pkg/adaptor"
	"git.compose.io/compose/transporter/pkg/transporter"
	"github.com/nu7hatch/gouuid"
	"github.com/robertkrimen/otto"
)

// Node is a struct modelled after the transporter.Node struct, but
// more easily able to serialize to json for to use within the application.js
type Node struct {
	UUID       string
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	Extra      adaptor.Config `json:"extra"`
	Children   []*Node        `json:"children"`
	RootUUID   string
	ParentUUID string
}

// NewNode creates a node
func NewNode(name, kind string, extra adaptor.Config) (node Node, err error) {
	uuid, err := uuid.NewV4()
	if err != nil {
		return node, err
	}

	return Node{UUID: uuid.String(), Name: name, Type: kind, Extra: extra, RootUUID: uuid.String(), Children: make([]*Node, 0), ParentUUID: uuid.String()}, nil
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
	node.ParentUUID = n.UUID
	n.Children = append(n.Children, node)
}

// Find a child node, depth first search
func (n *Node) Find(childUUID string) (node *Node, err error) {
	if n.UUID == childUUID {
		return n, nil
	}
	for _, child := range n.Children {
		if child.UUID == childUUID {
			return child, nil
		}
		found, err := child.Find(childUUID)
		if err != nil {
			continue
		}
		return found, nil
	}
	return nil, fmt.Errorf("child %s not found under %s", childUUID, n.UUID)
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
