package node

import (
	"fmt"

	"github.com/MongoHQ/transporter/pkg/pipe"
)

type Node struct {
	Name    string
	Type    string
	Uri     string
	Input   pipe.Pipe
	Outputs []egress
}

func NewNode(config NodeConfig) *Node {
	return &Node{Name: config.Name, Type: config.Type, Uri: config.Uri, Outputs: make([]egress, 0)}
}

func (n *Node) Register(name string, p pipe.Pipe) {
	n.Outputs = append(n.Outputs, egress{n: name, p: p})
}

func (n *Node) String() string {
	return fmt.Sprintf("%-20s %-15s %s", n.Name, n.Type, n.Uri)
}

/*
 *
 * name the pipes
 *
 */
type egress struct {
	n string
	p pipe.Pipe
}
