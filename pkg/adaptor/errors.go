package adaptor

import (
	"fmt"

	"gopkg.in/mgo.v2/bson"
)

const (
	NOTICE ErrorLevel = iota
	WARNING
	FATAL
)

type ErrorLevel int

func levelToString(lvl ErrorLevel) string {
	switch lvl {
	case NOTICE:
		return "NOTICE"
	case WARNING:
		return "WARNING"
	case FATAL:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

type Error struct {
	Lvl    ErrorLevel
	Str    string
	Record bson.M
}

func NewError(lvl ErrorLevel, str string, record bson.M) Error {
	return Error{Lvl: lvl, Str: str, Record: record}
}

func (t Error) Error() string {
	return fmt.Sprintf("%s: %s", levelToString(t.Lvl), t.Str)
}
