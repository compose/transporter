// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package node provides all implemented functionality to move
// data through transporter.
package transporter

import (
	"errors"
	"fmt"
	"reflect"
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
	Start(Pipe) error
	Stop() error
	Config() ConfigNode
}

/*
 * A Config stores the list of nodes that are available to a transporter, as well
 * as information about the api
 */
type Config struct {
	Api struct {
		Uri             string `json:"uri" yaml:"uri"`
		MetricsInterval int    `json:"interval" yaml:"interval"`
	} `json:"api" yaml:"api"`
	Nodes map[string]ConfigNode
}

/*
 * A ConfigNode is a description of an endpoint.  This is not a concrete implementation of a data store, just a
 * container to hold config values.
 */
type ConfigNode struct {
	Role      NodeRole          `json:"role"`
	Name      string            `json:"name"`
	Type      string            `json:"type"`
	Uri       string            `json:"uri"`
	Namespace string            `json:"namespace"`
	Extra     map[string]string `json:"extra"`
}

func (n ConfigNode) String() string {
	return fmt.Sprintf("%-20s %-15s %-30s %s", n.Name, n.Type, n.Namespace, n.Uri)
}

/*
 * Create a concrete node that will read/write to a datastore based on the type
 * of node
 */
func (n *ConfigNode) Create() (Node, error) {

	fn, ok := Registry[n.Type]
	if !ok {
		return nil, fmt.Errorf("Node type '%s' is not defined", n.Type)
	}

	result := reflect.ValueOf(fn).Call([]reflect.Value{reflect.ValueOf(*n)})
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
	SOURCE               NodeRole = iota
	SINK                 NodeRole = iota
	SOMETHINGINTHEMIDDLE NodeRole = iota // TODO i'm tempted to leave it like this.. bug..
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
