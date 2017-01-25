// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package message provides wrapper structs and helper methods to pipe
// actual database documents throughout transporter.
package message

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
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
}

// From builds a message.Msg specific to an elasticsearch document
func From(op ops.Op, namespace string, d data.Data) Msg {
	return &Base{
		Operation: op,
		TS:        time.Now().Unix(),
		NS:        namespace,
		MapData:   d,
	}
}

// Base represents a standard message format for transporter data
// if it does not meet your need, you can embed the struct and override whatever
// methods needed to accurately represent the data structure.
type Base struct {
	TS        int64
	NS        string
	Operation ops.Op
	MapData   data.Data
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
	switch id := m.MapData["_id"].(type) {
	case string:
		return id
	case bson.ObjectId:
		return id.Hex()
	default:
		return fmt.Sprintf("%v", id)
	}
}

// MarshalData attempts to call json.Marshal on the Msg.
func MarshalData(m Msg) ([]byte, error) {
	return json.Marshal(m.Data())
}

// SplitNamespace splits the nessage namespace into its constituent fields
func SplitNamespace(m Msg) (string, string, error) {
	fields := strings.SplitN(m.Namespace(), ".", 2)

	if len(fields) != 2 {
		return "", "", fmt.Errorf("malformed msg namespace")
	}
	return fields[0], fields[1], nil
}
