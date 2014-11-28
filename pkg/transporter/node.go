// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package transporter provides all implemented functionality to move
// data through transporter.
package transporter

import (
	"fmt"
	"time"

	"github.com/compose/transporter/pkg/impl"
	"github.com/compose/transporter/pkg/pipe"
)

// A Node is the basic building blocks of transporter pipelines.
// Nodes are constructed in a tree, with the first node broadcasting
// data to each of it's children.
// Node tree's can be constructed as follows:
// 	source := transporter.NewNode("name1", "mongo", impl.ExtraConfig{"uri": "mongodb://localhost/boom", "namespace": "boom.foo", "debug": true})
// 	sink1 := transporter.NewNode("crapfile", "file", impl.ExtraConfig{"uri": "stdout://"})
// 	sink2 := transporter.NewNode("crapfile2", "file", impl.ExtraConfig{"uri": "stdout://"})

// 	source.Add(sink1)
// 	source.Add(sink2)
//
type Node struct {
	Name     string           `json:"name"`     // the name of this node
	Type     string           `json:"type"`     // the node's type, used to create the implementation
	Extra    impl.ExtraConfig `json:"extra"`    // extra config options that are passed to the implementation
	Children []*Node          `json:"children"` // the nodes are set up as a tree, this is an array of this nodes children
	Parent   *Node            `json:"parent"`   // this node's parent node, if this is nil, this is a 'source' node

	impl impl.Impl
	pipe *pipe.Pipe
}

func NewNode(name, kind string, extra map[string]interface{}) *Node {
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
		// s = fmt.Sprintf("%18s %-40s %-15s %-30s %s\n", " ", "Name", "Type", "Namespace", "Uri")
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

// Init sets up the node for action.  It creates a pipe and impl for this node,
// and then recurses down the tree calling Init on each child
func (n *Node) Init(interval time.Duration) (err error) { // TODO lets just pass in the duration here
	if n.Parent == nil { // we don't have a parent, we're the source
		n.pipe = pipe.NewPipe(nil, n.Path(), interval)
	} else { // we have a parent, so pass in the parent's pipe here
		n.pipe = pipe.NewPipe(n.Parent.pipe, n.Path(), interval)
	}

	n.impl, err = impl.CreateImpl(n.Type, n.Extra, n.pipe)
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

// Stop's this node's impl, and sends a stop to each child of this node
func (n *Node) Stop() {
	n.impl.Stop()
	for _, node := range n.Children {
		node.Stop()
	}
}

// Start starts the nodes children in a go routine, and then runs either Start() or Listen() on the
// node's impl
func (n *Node) Start() error {
	for _, child := range n.Children {
		go func(node *Node) {
			// pipeline.nodeWg.Add(1)
			node.Start()
			// pipeline.nodeWg.Done()
		}(child)
	}

	if n.Parent == nil {
		return n.impl.Start()
	}

	return n.impl.Listen()
}

func (n *Node) Validate() bool {

	// the root node should have children
	if n.Parent == nil && len(n.Children) == 0 {
		return false
	}

	// transformers need children
	if n.Type == "transformer" && len(n.Children) == 0 {
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
