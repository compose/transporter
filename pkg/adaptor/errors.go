package adaptor

import (
	"fmt"

	"gopkg.in/mgo.v2/bson"
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

type Error struct {
	Lvl    ErrorLevel
	Str    string
	Path   string
	Record bson.M
}

func NewError(lvl ErrorLevel, path, str string, record bson.M) Error {
	return Error{Lvl: lvl, Path: path, Str: str, Record: record}
}

func (t Error) Error() string {
	return fmt.Sprintf("%s: %s", levelToString(t.Lvl), t.Str)
}
