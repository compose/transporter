package main

import (
	"encoding/json"
	"fmt"

	"github.com/compose/transporter/pkg/transporter"
	"github.com/nu7hatch/gouuid"
	"github.com/robertkrimen/otto"
)

// A Config stores meta information about the transporter.  This contains a
// list of the the nodes that are available to a transporter (sources and sinks, not transformers)
// as well as information about the api used to handle transporter events, and the interval
// between metrics events.
type Config struct {
	Api   transporter.Api `json:"api" yaml:"api"`
	Nodes map[string]struct {
		Type string `json:"type" yaml:"type"`
		Uri  string `json:"uri" yaml:"uri"`
	}
}

type Node struct {
	Uuid     string
	Name     string                 `json:"name"`
	Type     string                 `json:"type"`
	Extra    map[string]interface{} `json:"extra"`
	Children []*Node                `json:"children"`
	RootUuid string
}

func NewNode(name, kind string, extra map[string]interface{}) (node Node, err error) {
	uuid, err := uuid.NewV4()
	if err != nil {
		return node, err
	}

	return Node{Uuid: uuid.String(), Name: name, Type: kind, Extra: extra, RootUuid: uuid.String(), Children: make([]*Node, 0)}, nil
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
func (n *Node) AddNode(node *Node) {
	node.RootUuid = n.RootUuid
	n.Children = append(n.Children, node)
}

func (n *Node) CreateTransporterNode() *transporter.Node {
	self := transporter.NewNode(n.Name, n.Type, n.Extra)

	for _, child := range n.Children {
		self.Attach(child.CreateTransporterNode())
	}

	return self
}
