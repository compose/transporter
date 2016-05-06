// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package transporter provides all adaptoremented functionality to move
// data through transporter.
package transporter

import (
	"fmt"
	"time"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/pipe"
)

// A Node is the basic building blocks of transporter pipelines.
// Nodes are constructed in a tree, with the first node broadcasting
// data to each of it's children.
// Node tree's can be constructed as follows:
// 	source := transporter.NewNode("name1", "mongo", adaptor.Config{"uri": "mongodb://localhost/boom", "namespace": "boom.foo", "debug": true})
// 	sink1 := transporter.NewNode("foofile", "file", adaptor.Config{"uri": "stdout://"})
// 	sink2 := transporter.NewNode("foofile2", "file", adaptor.Config{"uri": "stdout://"})
// 	source.Add(sink1)
// 	source.Add(sink2)
//
type Node struct {
	Name     string         `json:"name"`     // the name of this node
	Type     string         `json:"type"`     // the node's type, used to create the adaptorementation
	Extra    adaptor.Config `json:"extra"`    // extra config options that are passed to the adaptorementation
	Children []*Node        `json:"children"` // the nodes are set up as a tree, this is an array of this nodes children
	Parent   *Node          `json:"parent"`   // this node's parent node, if this is nil, this is a 'source' node

	adaptor adaptor.StopStartListener
	pipe    *pipe.Pipe
}

// NewNode creates a new Node struct
func NewNode(name, kind string, extra adaptor.Config) *Node {
	return &Node{
		Name:     name,
		Type:     kind,
		Extra:    extra,
		Children: make([]*Node, 0),
	}
}

// String
func (n *Node) String() string {
	var (
		uri       string
		s         string
		prefix    string
		namespace = n.Extra.GetString("namespace")
		depth     = n.depth()
	)
	if n.Type == "transformer" {
		uri = n.Extra.GetString("filename")
	} else {
		uri = n.Extra.GetString("uri")
	}

	prefixformatter := fmt.Sprintf("%%%ds%%-%ds", depth, 18-depth)

	if n.Parent == nil { // root node
		// s = fmt.Sprintf("%18s %-40s %-15s %-30s %s\n", " ", "Name", "Type", "Namespace", "URI")
		prefix = fmt.Sprintf(prefixformatter, " ", "- Source: ")
	} else if len(n.Children) == 0 {
		prefix = fmt.Sprintf(prefixformatter, " ", "- Sink: ")
	} else if n.Type == "transformer" {
		prefix = fmt.Sprintf(prefixformatter, " ", "- Transformer: ")
	}

	s += fmt.Sprintf("%-18s %-40s %-15s %-30s %s", prefix, n.Name, n.Type, namespace, uri)

	for _, child := range n.Children {
		s += "\n" + child.String()
	}
	return s
}

// depth is a measure of how deep into the node tree this node is.  Used to indent the String() stuff
func (n *Node) depth() int {
	if n.Parent == nil {
		return 1
	}

	return 1 + n.Parent.depth()
}

// Path returns a string representation of the names of all the node's parents concatenated with "/"  used in metrics
// eg. for the following tree
// source := transporter.NewNode("name1", "mongo", adaptor.Config{"uri": "mongodb://localhost/boom", "namespace": "boom.foo", "debug": true})
// 	sink1 := transporter.NewNode("foofile", "file", adaptor.Config{"uri": "stdout://"})
// 	source.Add(sink1)
// 'source' will have a Path of 'name1', and 'sink1' will have a path of 'name1/sink1'
func (n *Node) Path() string {
	if n.Parent == nil {
		return n.Name
	}

	return n.Parent.Path() + "/" + n.Name
}

// Add the given node as a child of this node.
// This has side effects, and sets the parent of the given node
func (n *Node) Add(node *Node) *Node {
	node.Parent = n
	n.Children = append(n.Children, node)
	return n
}

// Init sets up the node for action.  It creates a pipe and adaptor for this node,
// and then recurses down the tree calling Init on each child
func (n *Node) Init(interval time.Duration) (err error) {
	path := n.Path()
	if n.Parent == nil { // we don't have a parent, we're the source
		n.pipe = pipe.NewPipe(nil, path)
	} else { // we have a parent, so pass in the parent's pipe here
		n.pipe = pipe.NewPipe(n.Parent.pipe, path)
	}

	n.adaptor, err = adaptor.Createadaptor(n.Type, path, n.Extra, n.pipe)
	if err != nil {
		return err
	}

	for _, child := range n.Children {
		err = child.Init(interval) // init each child
		if err != nil {
			return err
		}
	}

	return nil
}

// Stop this node's adaptor, and sends a stop to each child of this node
func (n *Node) Stop() {
	for _, node := range n.Children {
		node.Stop()
	}
	n.adaptor.Stop()
}

// Start starts the nodes children in a go routine, and then runs either Start() or Listen()
// on the node's adaptor.  Root nodes (nodes with no parent) will run Start()
// and will emit messages to it's children,
// All descendant nodes run Listen() on the adaptor
func (n *Node) Start() error {
	for _, child := range n.Children {
		go func(node *Node) {
			node.Start()
		}(child)
	}

	if n.Parent == nil {
		return n.adaptor.Start()
	}

	return n.adaptor.Listen()
}

// Validate ensures that the node tree conforms to a proper structure.
// Node trees must have at least one source, and one sink.
// dangling transformers are forbidden.  Validate only knows about default adaptors
// in the adaptor package, it can't validate any custom adaptors
func (n *Node) Validate() bool {
	if n.Parent == nil && len(n.Children) == 0 { // the root node should have children
		return false
	}

	if n.Type == "transformer" && len(n.Children) == 0 { // transformers need children
		return false
	}

	for _, child := range n.Children {
		if !child.Validate() {
			return false
		}
	}
	return true
}

// Endpoints recurses down the node tree and accumulates a map associating node name with node type
// this is primarly used with the boot event
func (n *Node) Endpoints() map[string]string {
	m := map[string]string{n.Name: n.Type}
	for _, child := range n.Children {
		childMap := child.Endpoints()
		for k, v := range childMap {
			m[k] = v
		}
	}
	return m
}
