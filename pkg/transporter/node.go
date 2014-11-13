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

	"github.com/compose/transporter/pkg/pipe"
)

var NoNodeError = errors.New("Module not found")

var (
	Registry = map[string]interface{}{
		"mongo":         NewMongoImpl,
		"file":          NewFileImpl,
		"elasticsearch": NewElasticsearchImpl,
		"influx":        NewInfluxImpl,
		"transformer":   NewTransformer,
	}
)

/*
 * All nodes must implement the Node interface
 */
type Node interface {
	Start(pipe.Pipe) error
	Stop() error
}

// A Config stores meta information about the transporter.  This contains a
// list of the the nodes that are available to a transporter (sources and sinks, not transformers)
// as well as information about the api used to handle transporter events, and the interval
// between metrics events.
type Config struct {
	Api struct {
		Uri             string `json:"uri" yaml:"uri"`
		MetricsInterval int    `json:"interval" yaml:"interval"`
	} `json:"api" yaml:"api"`
	Nodes map[string]ConfigNode
}

//
// A ConfigNode is a description of an endpoint.  This is not a concrete implementation of a data store, just a
// container to hold config values.

type ConfigNode struct {
	Role  NodeRole               `json:"role"`
	Name  string                 `json:"name"`
	Type  string                 `json:"type"`
	Uri   string                 `json:"uri"`
	Extra map[string]interface{} `json:"extra"`
}

func (n ConfigNode) String() string {
	return fmt.Sprintf("%-20s %-15s %-30s %s", n.Name, n.Type, n.Extra["namespace"].(string), n.Extra["uri"].(string))
}

// Create a concrete node that will read/write to a datastore based on the type
// of node.
//
// Node types are stored in the node registry and we generate the correct type of Node by examining the NodeConfig.Type
// property to find the node's constructore.
//
// Each constructor is assumed to be of the form
// func NewImpl(name, namespace, uri string, role NodeRole, extra map[string]interface{}) (*Impl, error) {
func (n *ConfigNode) Create() (Node, error) {

	fn, ok := Registry[n.Type]
	if !ok {
		return nil, fmt.Errorf("Node type '%s' is not defined", n.Type)
	}

	args := []reflect.Value{
		reflect.ValueOf(n.Role),
		reflect.ValueOf(n.Extra),
	}

	result := reflect.ValueOf(fn).Call(args)
	impl := result[0]
	inter := result[1].Interface()

	if inter != nil {
		return nil, inter.(error)
	}

	switch m := impl.Interface().(type) {
	case *MongoImpl:
		return m, nil
	case *FileImpl:
		return m, nil
	case *ElasticsearchImpl:
		return m, nil
	case *InfluxImpl:
		return m, nil
	case *Transformer:
		return m, nil
	}

	return nil, NoNodeError
}

/*
 * TODO not sure if this makes sense to be part of the node.  this might be better to be part of the pipeline
 */
type NodeRole int

const (
	SOURCE      NodeRole = iota
	SINK        NodeRole = iota
	TRANSFORMER NodeRole = iota // TODO i'm tempted to leave it like this.. bug..
)

func (n NodeRole) String() string {
	switch n {
	case SOURCE:
		return "Source"
	case SINK:
		return "Sink"
	default:
		return "Other"
	}
}
