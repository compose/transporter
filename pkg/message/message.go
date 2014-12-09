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

var (
	idKeys = []string{"_id", "id"}
)

// A Msg serves to wrap the actual document to
// provide additional metadata about the document
// being transported.
type Msg struct {
	Timestamp  int64
	Op         OpType
	ID         interface{}
	OriginalID interface{}
	document   bson.M // document is private
	idKey      string // where the original id value is stored, either "_id" or "id"
}

// NewMsg returns a new Msg with the ID extracted
// from the original document
func NewMsg(op OpType, doc bson.M) *Msg {
	m := &Msg{
		Timestamp: time.Now().Unix(),
		Op:        op,
	}
	if doc != nil {
		m.document, m.ID = m.extractID(doc)
		m.OriginalID = m.ID
	}

	return m
}

// extractID will handle separating the id field from the
// rest of the document, can handle both 'id' and '_id'
func (m *Msg) extractID(doc bson.M) (bson.M, interface{}) {
	for _, key := range idKeys {
		id, exists := doc[key]
		if exists {
			m.idKey = key
			delete(doc, key)
			return doc, id
		}
	}

	fmt.Printf("id not found %+v\n", doc)
	return doc, nil
}

// IDString returns the original id as a string value
func (m *Msg) IDString() string {
	switch t := m.ID.(type) {
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

// Document returns the original doc, unaltered
func (m *Msg) Document() bson.M {
	return m.DocumentWithID(m.idKey)
}

// SetDocument will set the document variable and
// extract out the id and preserve it
func (m *Msg) SetDocument(doc bson.M) {
	m.document, m.ID = m.extractID(doc)
	if m.OriginalID == nil { // if we don't have an original id, then set it here
		m.OriginalID = m.ID
	}
}

// DocumentWithID returns the document with the id field
// attached to the specified key
func (m *Msg) DocumentWithID(key string) bson.M {
	doc := m.document
	if m.ID != nil {
		doc[key] = m.ID
	}
	return doc
}
