// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pipeline provides all adaptoremented functionality to move
// data through transporter.
package pipeline

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/function"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
	"github.com/compose/transporter/pipe"
)

var (
	transformerNode = "transformer"
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
	Name       string  `json:"name"`     // the name of this node
	Type       string  `json:"type"`     // the node's type, used to create the adaptorementation
	Children   []*Node `json:"children"` // the nodes are set up as a tree, this is an array of this nodes children
	Parent     *Node   `json:"parent"`   // this node's parent node, if this is nil, this is a 'source' node
	Transforms []*Transform

	nsFilter *regexp.Regexp
	c        client.Client
	reader   client.Reader
	writer   client.Writer
	done     chan struct{}
	wg       sync.WaitGroup
	l        log.Logger
	pipe     *pipe.Pipe
}

type Transform struct {
	Name     string
	Fn       function.Function
	NsFilter *regexp.Regexp
}

// NewNode creates a new Node struct
func NewNode(name, kind, ns string, a adaptor.Adaptor, parent *Node) (*Node, error) {
	compiledNs, err := regexp.Compile(strings.Trim(ns, "/"))
	if err != nil {
		return nil, err
	}
	n := &Node{
		Name:       name,
		Type:       kind,
		nsFilter:   compiledNs,
		Children:   make([]*Node, 0),
		Transforms: make([]*Transform, 0),
		done:       make(chan struct{}),
	}

	n.c, err = a.Client()
	if err != nil {
		return nil, err
	}

	if parent == nil {
		// TODO: remove path param
		n.pipe = pipe.NewPipe(nil, "")
		n.reader, err = a.Reader()
		if err != nil {
			return nil, err
		}
	} else {
		n.Parent = parent
		// TODO: remove path param
		n.pipe = pipe.NewPipe(parent.pipe, "")
		parent.Children = append(parent.Children, n)
		n.writer, err = a.Writer(n.done, &n.wg)
		if err != nil {
			return nil, err
		}
	}

	return n, nil
}

// String
func (n *Node) String() string {
	var (
		s, prefix string
		depth     = n.depth()
	)

	prefixformatter := fmt.Sprintf("%%%ds%%-%ds", depth, 18-depth)

	if n.Parent == nil { // root node
		prefix = fmt.Sprintf(prefixformatter, " ", "- Source: ")
	} else {
		prefix = fmt.Sprintf(prefixformatter, " ", "- Sink: ")
	}

	s += fmt.Sprintf("%s %-40s %-15s %-30s", prefix, n.Name, n.Type, n.nsFilter.String())

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

// Start starts the nodes children in a go routine, and then runs either Start() or Listen()
// on the node's adaptor.  Root nodes (nodes with no parent) will run Start()
// and will emit messages to it's children,
// All descendant nodes run Listen() on the adaptor
func (n *Node) Start() error {
	path := n.Path()
	n.l = log.With("name", n.Name).With("type", n.Type).With("path", path)

	for _, child := range n.Children {
		go func(node *Node) {
			node.Start()
		}(child)
	}

	if n.Parent == nil {
		return n.start()
	}

	return n.listen()
}

// Start the adaptor as a source
func (n *Node) start() (err error) {
	n.l.Infoln("adaptor Starting...")

	s, err := n.c.Connect()
	if err != nil {
		return err
	}
	if closer, ok := s.(client.Closer); ok {
		defer func() {
			n.l.Infoln("closing session...")
			closer.Close()
			n.l.Infoln("session closed...")
		}()
	}
	readFunc := n.reader.Read(func(check string) bool { return n.nsFilter.MatchString(check) })
	msgChan, err := readFunc(s, n.done)
	if err != nil {
		return err
	}
	for msg := range msgChan {
		n.pipe.Send(msg)
	}

	n.l.Infoln("adaptor Start finished...")
	return nil
}

func (n *Node) listen() (err error) {
	n.l.Infoln("adaptor Listening...")
	defer n.l.Infoln("adaptor Listen closed...")

	return n.pipe.Listen(n.write, n.nsFilter)
}

func (n *Node) write(msg message.Msg) (message.Msg, error) {
	transformedMsg, err := n.applyTransforms(msg)
	if err != nil {
		return msg, nil
	} else if transformedMsg == nil {
		return nil, nil
	}
	sess, err := n.c.Connect()
	if err != nil {
		return msg, err
	}
	defer func() {
		if s, ok := sess.(client.Closer); ok {
			s.Close()
		}
	}()
	returnMsg, err := n.writer.Write(transformedMsg)(sess)
	if err != nil {
		n.pipe.Err <- adaptor.Error{
			Lvl:    adaptor.ERROR,
			Path:   n.Path(),
			Err:    fmt.Sprintf("write message error (%s)", err),
			Record: msg.Data,
		}
	}
	return returnMsg, err
}

func (n *Node) applyTransforms(msg message.Msg) (message.Msg, error) {
	if msg.OP() != ops.Command {
		for _, transform := range n.Transforms {
			if !transform.NsFilter.MatchString(msg.Namespace()) {
				n.l.With("transform", transform.Name).With("ns", msg.Namespace()).Infoln("filtered message")
				continue
			}
			m, err := transform.Fn.Apply(msg)
			if err != nil {
				n.l.Errorf("transform function error, %s", err)
				return nil, err
			} else if m == nil {
				n.l.With("transform", transform.Name).Infoln("returned nil message, skipping")
				return nil, nil
			}
			msg = m
			if msg.OP() == ops.Skip {
				n.l.With("transform", transform.Name).With("op", msg.OP()).Infoln("skipping message")
				return nil, nil
			}
		}
	}
	return msg, nil
}

// Stop this node's adaptor, and sends a stop to each child of this node
func (n *Node) Stop() {
	n.stop()
	for _, node := range n.Children {
		node.Stop()
	}
}

func (n *Node) stop() error {
	n.l.Infoln("adaptor Stopping...")
	n.pipe.Stop()

	close(n.done)
	n.wg.Wait()

	if closer, ok := n.writer.(client.Closer); ok {
		defer func() {
			n.l.Infoln("closing writer...")
			closer.Close()
			n.l.Infoln("writer closed...")
		}()
	}
	if closer, ok := n.c.(client.Closer); ok {
		defer func() {
			n.l.Infoln("closing connection...")
			closer.Close()
			n.l.Infoln("connection closed...")
		}()
	}

	n.l.Infoln("adaptor Stopped")
	return nil
}

// Validate ensures that the node tree conforms to a proper structure.
// Node trees must have at least one source, and one sink.
// dangling transformers are forbidden.  Validate only knows about default adaptors
// in the adaptor package, it can't validate any custom adaptors
func (n *Node) Validate() bool {
	if n.Parent == nil && len(n.Children) == 0 { // the root node should have children
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
// this is primarily used with the boot event
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
