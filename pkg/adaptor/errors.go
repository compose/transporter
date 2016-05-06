package adaptor

import (
	"fmt"
)

// Adaptor errors have levels to indicate their severity.
// CRITICAL errors indicate that the program cannot continue running.
//
// ERROR errors indicate a problem with a specific document or message.
// a document might not have been applied properly to a source, but the program can continue
//
// WARNING Todo
//
// NOTICE ToDo
const (
	NOTICE ErrorLevel = iota
	WARNING
	ERROR
	CRITICAL
)

// ErrorLevel indicated the severity of the error
type ErrorLevel int

func levelToString(lvl ErrorLevel) string {
	switch lvl {
	case NOTICE:
		return "NOTICE"
	case WARNING:
		return "WARNING"
	case ERROR:
		return "ERROR"
	case CRITICAL:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

// Error is an error that happened during an adaptor's operation.
// Error's include both an indication of the severity, Level, as well as
// a reference to the Record that was in process when the error occured
type Error struct {
	Lvl    ErrorLevel
	Str    string
	Path   string
	Record interface{}
}

// NewError creates an Error type with the specificed level, path, message and record
func NewError(lvl ErrorLevel, path, str string, record interface{}) Error {
	return Error{Lvl: lvl, Path: path, Str: str, Record: record}
}

// Error returns the error as a string
func (t Error) Error() string {
	return fmt.Sprintf("%s: %s", levelToString(t.Lvl), t.Str)
}
