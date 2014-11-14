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

	"github.com/compose/transporter/pkg/impl"
	"github.com/compose/transporter/pkg/pipe"
)

var NoNodeError = errors.New("Module not found")

/*
 * All nodes must implement the Node interface
 */
type Node interface {
	Listen() error
	Stop() error
}

type Source interface {
	Start() error
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
	Name  string                 `json:"name"`
	Type  string                 `json:"type"`
	Uri   string                 `json:"uri"`
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

//
func (n ConfigNode) callCreator(pipe pipe.Pipe, fn interface{}) (reflect.Value, error) {

	args := []reflect.Value{
		reflect.ValueOf(pipe),
		reflect.ValueOf(n.Extra),
	}

	result := reflect.ValueOf(fn).Call(args)
	node := result[0]
	inter := result[1].Interface()

	if inter != nil {
		return node, inter.(error)
	}

	return node, nil
}

// Create a concrete node that will listen on a pipe.  An implementation of the Node interface.  These types are generally either sinks or transformers
//
// Node types are stored in the node registry and we generate the correct type of Node by examining the NodeConfig.Type
// property to find the node's constructore.
//
// Each constructor is assumed to be of the form
// func NewImpl(pipe pipe.Pipe, extra map[string]interface{}) (*Impl, error) {
func (n *ConfigNode) Create(p pipe.Pipe) (node Node, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Cannot create Node, %v", r)
		}
	}()

	var Registry = map[string]interface{}{
		"mongo":         impl.NewMongodb,
		"file":          impl.NewFile,
		"elasticsearch": impl.NewElasticsearch,
		"influx":        impl.NewInfluxdb,
		"transformer":   impl.NewTransformer,
	}

	fn, ok := Registry[n.Type]
	if !ok {
		return nil, fmt.Errorf("Node type '%s' is not defined", n.Type)
	}

	val, err := n.callCreator(p, fn)
	if err != nil {
		return nil, err
	}

	return val.Interface().(Node), nil
}

// Create a concrete node that will act as a source and transmit data through a transporter Pipeline.  An implementation of the Source interface.  These types are generally either sinks or transformers
//
// Node types are stored in the node registry and we generate the correct type of Node by examining the NodeConfig.Type
// property to find the node's constructore.
//
// Each constructor is assumed to be of the form
// func NewImpl(pipe pipe.Pipe, extra map[string]interface{}) (*Impl, error) {
func (n *ConfigNode) CreateSource(p pipe.Pipe) (source Source, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Cannot create Source, %v", r)
		}
	}()

	var Registry = map[string]interface{}{
		"mongo": impl.NewMongodb,
		"file":  impl.NewFile,
	}

	fn, ok := Registry[n.Type]
	if !ok {
		return nil, fmt.Errorf("Source type '%s' is not defined", n.Type)
	}
	val, err := n.callCreator(p, fn)
	if err != nil {
		return nil, err
	}

	return val.Interface().(Source), nil
}
