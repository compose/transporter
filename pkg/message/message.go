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
)

// A Msg serves to wrap the actual document to
// provide additional metadata about the document
// being transported.
type Msg struct {
	Timestamp int64
	Op        OpType
	Document  bson.M
}

// NewMsg returns a new Msg with the ID extracted
// from the original document
func NewMsg(op OpType, doc bson.M) *Msg {
	m := &Msg{
		Timestamp: time.Now().Unix(),
		Op:        op,
		Document:  doc,
	}

	return m
}

// IDString returns the original id as a string value
func (m *Msg) IDString(key string) string {
	id, ok := m.Document[key]
	if !ok {
		return ""
	}
	switch t := id.(type) {
	case string:
		return t
	case bson.ObjectId:
		return t.Hex()
	case int32, int64, uint32, uint64:
		return fmt.Sprintf("%d", t)
	case float32, float64:
		return fmt.Sprintf("%f", t)
	default:
		return fmt.Sprintf("%v", t)
	}
}
