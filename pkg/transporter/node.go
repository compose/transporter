// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package transporter provides all implemented functionality to move
// data through transporter.
package transporter

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/compose/transporter/pkg/impl"
	"github.com/compose/transporter/pkg/pipe"
)

var (
	// The node was not found in the map
	MissingNodeError = errors.New("Node not defined")
)

var (
	nodeRegistry = map[string]interface{}{
		"mongo":         impl.NewMongodb,
		"file":          impl.NewFile,
		"elasticsearch": impl.NewElasticsearch,
		"influx":        impl.NewInfluxdb,
		"transformer":   impl.NewTransformer,
	}
)

// All nodes must implement the Node interface
type NodeImpl interface {
	Start() error
	Listen() error
	Stop() error
}

// An Api is the definition of the remote endpoint that receieves event and error posts
type Api struct {
	Uri             string `json:"uri" yaml:"uri"`
	MetricsInterval int    `json:"interval" yaml:"interval"`
	Key             string `json:"key" yaml:"key"`
	Pid             string `json:"pid" yaml:"pid"`
}

// A Node is the basic building blocks of transporter pipelines.
// Nodes are constructed in a tree, with the first node broadcasting
// data to each of it's children.
// Node tree's can be constructed as follows:
// 	source := transporter.NewNode("name1", "mongo", map[string]interface{}{"uri": "mongodb://localhost/boom", "namespace": "boom.foo", "debug": true})
// 	sink1 := transporter.NewNode("crapfile", "file", map[string]interface{}{"uri": "stdout://"})
// 	sink2 := transporter.NewNode("crapfile2", "file", map[string]interface{}{"uri": "stdout://"})

// 	source.Attach(sink1)
// 	source.Attach(sink2)
//
type Node struct {
	Name     string                 `json:"name"`     // the name of this node
	Type     string                 `json:"type"`     // the node's type, used to create the implementation
	Extra    map[string]interface{} `json:"extra"`    // extra config options that are passed to the implementation
	Children []*Node                `json:"children"` // the nodes are set up as a tree, this is an array of this nodes children
	Parent   *Node                  `json:"parent"`   // this node's parent node, if this is nil, this is a 'source' node

	impl NodeImpl
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

func (n *Node) String() string {
	uri, ok := n.Extra["uri"]
	if !ok {
		uri = "no uri set"
	}

	namespace, ok := n.Extra["namespace"]
	if !ok {
		namespace = "no namespace set"
	}
	return fmt.Sprintf("%-20s %-15s %-30s %s", n.Name, n.Type, namespace, uri)
}

func (n *Node) Attach(node *Node) {
	node.Parent = n
	n.Children = append(n.Children, node)
}

func (n *Node) createImpl(p *pipe.Pipe) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("cannot create node: %v", r)
		}
	}()

	fn, ok := nodeRegistry[n.Type]
	if !ok {
		return MissingNodeError
	}

	args := []reflect.Value{
		reflect.ValueOf(p),
		reflect.ValueOf(n.Extra),
	}

	result := reflect.ValueOf(fn).Call(args)

	val := result[0]
	inter := result[1].Interface()

	if inter != nil {
		return inter.(error)
	}

	n.impl = val.Interface().(NodeImpl)

	return err
}

func (n *Node) Init(api Api) {
	if n.Parent == nil { // we don't have a parent, we're the source
		n.pipe = pipe.NewPipe(nil, n.Name, time.Duration(api.MetricsInterval)*time.Millisecond)
	} else { // we have a parent, so pass in the parent's pipe here
		n.pipe = pipe.NewPipe(n.Parent.pipe, n.Name, time.Duration(api.MetricsInterval)*time.Millisecond)
	}

	n.createImpl(n.pipe)

	for _, child := range n.Children {
		child.Init(api) // init each child
	}
}

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
