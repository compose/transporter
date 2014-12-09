package main

import (
	"encoding/json"
	"fmt"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/transporter"
	"github.com/nu7hatch/gouuid"
	"github.com/robertkrimen/otto"
)

type Node struct {
	UUID     string
	Name     string         `json:"name"`
	Type     string         `json:"type"`
	Extra    adaptor.Config `json:"extra"`
	Children []*Node        `json:"children"`
	RootUUID string
}

func NewNode(name, kind string, extra adaptor.Config) (node Node, err error) {
	uuid, err := uuid.NewV4()
	if err != nil {
		return node, err
	}

	return Node{UUID: uuid.String(), Name: name, Type: kind, Extra: extra, RootUUID: uuid.String(), Children: make([]*Node, 0)}, nil
}

func CreateNode(val interface{}) (Node, error) {
	t := Node{}
	ba, err := json.Marshal(val)

	if err != nil {
		return t, err
	}

	err = json.Unmarshal(ba, &t)
	return t, err
}

// turn this pipeline into an otto Object
func (n *Node) Object() (*otto.Object, error) {
	vm := otto.New()
	ba, err := json.Marshal(n)
	if err != nil {
		return nil, err
	}

	return vm.Object(fmt.Sprintf(`(%s)`, string(ba)))
}

// Add node adds a node as a child of the current node
func (n *Node) Add(node *Node) {
	node.RootUUID = n.RootUUID
	n.Children = append(n.Children, node)
}

func (n *Node) CreateTransporterNode() *transporter.Node {
	self := transporter.NewNode(n.Name, n.Type, n.Extra)

	for _, child := range n.Children {
		self.Add(child.CreateTransporterNode())
	}

	return self
}
