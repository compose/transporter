// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package message provides wrapper structs and helper methods to pipe
// actual database documents throughout transporter.
package message

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"gopkg.in/mgo.v2/bson"
)

// A Msg serves to wrap the actual document to
// provide additional metadata about the document
// being transported.
type Msg struct {
	Timestamp int64
	Op        OpType
	Data      interface{}
	Namespace string
}

// NewMsg returns a new Msg with the ID extracted
// from the original document
func NewMsg(op OpType, data interface{}, namespace string) *Msg {
	m := &Msg{
		Timestamp: time.Now().Unix(),
		Op:        op,
		Data:      data,
		Namespace: namespace,
	}

	return m
}

func (m *Msg) MatchNamespace(nsFilter *regexp.Regexp) (bool, error) {
	_, ns, err := m.SplitNamespace()
	if err != nil {
		return false, err
	}

	return nsFilter.MatchString(ns), nil
}

func (m *Msg) SplitNamespace() (string, string, error) {
	fields := strings.SplitN(m.Namespace, ".", 2)

	if len(fields) != 2 {
		return "", "", fmt.Errorf("malformed msg namespace")
	}
	return fields[0], fields[1], nil
}

// IsMap returns a bool indicating whether or not the msg.Data is maplike, i.e. a map[string]interface
// or a bson.M
func (m *Msg) IsMap() bool {
	switch m.Data.(type) {
	case map[string]interface{}, bson.M:
		return true
	default:
		return false
	}
}

// Map casts the Msg.Data into a map[string]interface{}
func (m *Msg) Map() map[string]interface{} {
	switch d := m.Data.(type) {
	case map[string]interface{}:
		return d
	case bson.M:
		return map[string]interface{}(d)
	default:
		return nil
	}
}

// IDString returns the original id as a string value
func (m *Msg) IDString(key string) (string, error) {
	doc := m.Map()
	if doc == nil {
		return "", fmt.Errorf("data is not a map")
	}
	id, ok := doc[key]
	if !ok {
		return "", fmt.Errorf("no key %s found in Data", key)
	}
	switch t := id.(type) {
	case string:
		return t, nil
	case bson.ObjectId:
		return t.Hex(), nil
	case int32, int64, uint32, uint64:
		return fmt.Sprintf("%d", t), nil
	case float32, float64:
		return fmt.Sprintf("%f", t), nil
	default:
		return fmt.Sprintf("%v", t), nil
	}
}
