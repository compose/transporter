// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package message provides wrapper structs and helper methods to pipe
// actual database documents throughout transporter.
package message

import (
	"fmt"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
)

// A Msg serves to wrap the actual document to
// provide additional metadata about the document
// being transported.
type Msg interface {
	ID() string
	OP() ops.Op
	Timestamp() int64
	Data() data.Data
	Namespace() string
	Confirms() chan struct{}
}

// From builds a message.Msg specific to an elasticsearch document
func From(op ops.Op, namespace string, d data.Data) Msg {
	return &Base{
		Operation: op,
		TS:        time.Now().Unix(),
		NS:        namespace,
		MapData:   d,
		confirm:   nil,
	}
}

// WithConfirms attaches a channel to be able to acknowledge message processing.
func WithConfirms(confirm chan struct{}, msg Msg) Msg {
	switch m := msg.(type) {
	case *Base:
		m.confirm = confirm
	}
	return msg
}

// Base represents a standard message format for transporter data
// if it does not meet your need, you can embed the struct and override whatever
// methods needed to accurately represent the data structure.
type Base struct {
	TS        int64
	NS        string
	Operation ops.Op
	MapData   data.Data
	confirm   chan struct{}
}

// Timestamp returns the time the object was created in transporter (i.e. it has no correlation
// with any time in the database).
func (m *Base) Timestamp() int64 {
	return m.TS
}

// Namespace returns the combination of database/table/colleciton for the underlying adaptor.
func (m *Base) Namespace() string {
	return m.NS
}

// OP returns the type of operation the message is associated with (i.e. insert/update/delete).
func (m *Base) OP() ops.Op {
	return m.Operation
}

// Data returns the internal representation of the document as the data.Data type
func (m *Base) Data() data.Data {
	return m.MapData
}

// ID will attempt to convert the _id field into a string representation
func (m *Base) ID() string {
	if _, ok := m.MapData["_id"]; !ok {
		return ""
	}
	switch id := m.MapData["_id"].(type) {
	case string:
		return id
	case bson.ObjectId:
		return id.Hex()
	default:
		return fmt.Sprintf("%v", id)
	}
}

func (m *Base) Confirms() chan struct{} {
	return m.confirm
}
