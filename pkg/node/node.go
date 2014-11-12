// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package node provides all implemented functionality to move
// data through transporter.
package node

import (
	"fmt"
)

type NodeRole int

const (
	SOURCE NodeRole = iota
	SINK   NodeRole = iota
)

func (n NodeRole) String() string {
	switch n {
	case SOURCE:
		return "Source"
	case SINK:
		return "Sink"
	default:
		return ""
	}
}

/*
 * A Node is an endpoint, Either a source, or a sink
 */
type Node struct {
	Role      NodeRole `json:"-"`
	Name      string   `json:"name"`
	Type      string   `json:"type"`
	Uri       string   `json:"uri"`
	Namespace string   `json:"namespace"`
	NodeImpl  NodeImpl `json:"-"`
}

func (n *Node) String() string {
	return fmt.Sprintf("%-20s %-15s %-30s %s", n.Name, n.Type, n.Namespace, n.Uri)
}

/*
 * Tie this to the actual implementation
 */
func (n *Node) Create(role NodeRole) (err error) {
	n.Role = role

	n.NodeImpl, err = NewImpl(n)
	return err
}
