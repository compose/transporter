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
	Data() interface{}
	Namespace() string
}

func MarshalData(m Msg) ([]byte, error) {
	d := m.Data()
	switch d.(type) {
	case data.SQLData, data.MapData, data.BSONData, data.CommandData:
		return json.Marshal(d)
	}
	return nil, fmt.Errorf("invalid data type for marshal: %T", d)
}

// SplitNamespace splits the nessage namespace into its constituent fields
func SplitNamespace(m Msg) (string, string, error) {
	fields := strings.SplitN(m.Namespace(), ".", 2)

	if len(fields) != 2 {
		return "", "", fmt.Errorf("malformed msg namespace")
	}
	return fields[0], fields[1], nil
}
