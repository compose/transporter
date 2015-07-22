// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package message

// OpType represents the many different Operations being
// performed against a document (i.e. Insert, Update, etc.)
type OpType int

// messages refer to specific types database operations which are enumerated here.
const (
	Insert OpType = iota
	Update
	Delete
	Command
	Noop
	Unknown
)

// String returns the constant of the
// string representation of the OpType object.
func (o OpType) String() string {
	switch o {
	case Insert:
		return "insert"
	case Update:
		return "update"
	case Delete:
		return "delete"
	case Command:
		return "command"
	case Noop:
		return "noop"
	default:
		return "unknown"
	}
}

// OpTypeFromString returns the constant
// representing the passed in string
func OpTypeFromString(s string) OpType {
	switch s[0] {
	case 'i':
		return Insert
	case 'u':
		return Update
	case 'd':
		return Delete
	case 'c':
		return Command
	case 'n':
		return Noop
	default:
		return Unknown
	}
}

// CommandType represents the different Commands capable
// of being executed against a database.
type CommandType int

// Transporter understands the following different command types
const (

	// Flush is interpreted by the recieving sink adaptors to attempt to flush all buffered
	// operations to the database.  This can be useful when switching from a copy to a tail operation
	Flush CommandType = iota
)
