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
	sourceRegistry = map[string]interface{}{
		"mongo": impl.NewMongodb,
		"file":  impl.NewFile,
	}

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

// Source nodes are used as the first element in the Pipeline chain
// TODO lose this or keep this?
type SourceImpl interface {
	Start() error
	Stop() error
}

// An Api is the definition of the remote endpoint that receieves event and error posts
type Api struct {
	Uri             string `json:"uri" yaml:"uri"`
	MetricsInterval int    `json:"interval" yaml:"interval"`
	Key             string `json:"key" yaml:"key"`
	Pid             string `json:"pid" yaml:"pid"`
}

// A ConfigNode is a description of an endpoint.  This is not a concrete implementation of a data store, just a
// container to hold config values.
type ConfigNode struct {
	Name  string                 `json:"name"`
	Type  string                 `json:"type"`
	Extra map[string]interface{} `json:"extra"`
}

func (n ConfigNode) String() string {
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

// callCreator will call the NewImpl method to create a new node or source
// func (n ConfigNode) callCreator(pipe pipe.Pipe, fn interface{}) (reflect.Value, error) {

// 	args := []reflect.Value{
// 		reflect.ValueOf(pipe),
// 		reflect.ValueOf(n.Extra),
// 	}

// 	result := reflect.ValueOf(fn).Call(args)
// 	node := result[0]
// 	inter := result[1].Interface()

// 	if inter != nil {
// 		return node, inter.(error)
// 	}

// 	return node, nil
// }

// Create a concrete node that will listen on a pipe.  An implementation of the Node interface.  These types are generally either sinks or transformers
//
// Node types are stored in the node registry and we generate the correct type of Node by examining the NodeConfig.Type
// property to find the node's constructore.
//
// Each constructor is assumed to be of the form
// func NewImpl(pipe pipe.Pipe, extra map[string]interface{}) (*Impl, error) {
// func (n *ConfigNode) Create(p pipe.Pipe) (node NodeImpl, err error) {
// 	defer func() {
// 		if r := recover(); r != nil {
// 			err = fmt.Errorf("cannot create node: %v", r)
// 		}
// 	}()

// 	fn, ok := nodeRegistry[n.Type]
// 	if !ok {
// 		return nil, MissingNodeError
// 	}

// 	val, err := n.callCreator(p, fn)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return val.Interface().(NodeImpl), nil
// }

// Create a concrete node that will act as a source and transmit data through a transporter Pipeline.  An implementation of the Source interface.  These types are generally either sinks or transformers
//
// Node types are stored in the node registry and we generate the correct type of Node by examining the NodeConfig.Type
// property to find the node's constructore.
//
// Each constructor is assumed to be of the form
// func NewImpl(pipe pipe.Pipe, extra map[string]interface{}) (*Impl, error) {
// func (n *ConfigNode) CreateSource(p pipe.Pipe) (source SourceImpl, err error) {
// 	defer func() {
// 		if r := recover(); r != nil {
// 			err = fmt.Errorf("cannot create node: %v", r)
// 		}
// 	}()

// 	fn, ok := sourceRegistry[n.Type]
// 	if !ok {
// 		return nil, MissingNodeError
// 	}
// 	val, err := n.callCreator(p, fn)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return val.Interface().(SourceImpl), nil
// }

/* TODO don't go breaking my heart */

type Node struct {
	Name     string                 `json:"name"`
	Type     string                 `json:"type"`
	Extra    map[string]interface{} `json:"extra"`
	Children []*Node                `json:"children"`
	Parent   *Node                  `json:"parent"`

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

func (n *Node) Attach(node *Node) {
	node.Parent = n
	n.Children = append(n.Children, node)
}

func (n *Node) actualize(p *pipe.Pipe) (err error) {
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
	// n.pipe = p

	return err
}

func (n *Node) DoTheThingWeNeedToDo(api Api) {
	if n.Parent == nil {
		// we're the source
		n.pipe = pipe.NewPipe(nil, n.Name, time.Duration(api.MetricsInterval)*time.Millisecond)
	} else {
		n.pipe = pipe.NewPipe(n.Parent.pipe, n.Name, time.Duration(api.MetricsInterval)*time.Millisecond)
	}

	n.actualize(n.pipe)

	for _, child := range n.Children {
		child.DoTheThingWeNeedToDo(api)
	}
}

func (n *Node) Stop() error {
	n.impl.Stop()
	for _, node := range n.Children {
		node.Stop()
	}
	return nil //TODO return an error
}

func (n *Node) Start() error {
	for _, child := range n.Children {
		go func(node *Node) {
			// pipeline.nodeWg.Add(1)
			node.Start()
			// pipeline.nodeWg.Done()
		}(child)
	}

	//TODO somewhat hacky, but the source node needs to call a different method
	// or else the impl needs to know whether it's source or sink
	if n.Parent == nil {
		return n.impl.Start()
	}

	return n.impl.Listen()
}

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
