package adaptor_test

import (
	"reflect"
	"testing"

	"github.com/compose/transporter/adaptor"
)

var errorLevelTests = []struct {
	e        adaptor.Error
	expected string
}{
	{adaptor.Error{0, "informational error", "", nil}, "NOTICE: informational error"},
	{adaptor.Error{1, "modest error", "", nil}, "WARNING: modest error"},
	{adaptor.Error{2, "error error", "", nil}, "ERROR: error error"},
	{adaptor.Error{3, "uhoh error", "", nil}, "CRITICAL: uhoh error"},
	{adaptor.Error{4, "not an error", "", nil}, "UNKNOWN: not an error"},
}

func TestNewError(t *testing.T) {
	for _, elt := range errorLevelTests {
		if !reflect.DeepEqual(elt.e.Error(), elt.expected) {
			t.Errorf("wrong Error(), expected %s, got %s", elt.expected, elt.e.Error())
		}
	}
}

var (
	errorTests = []struct {
		e        error
		expected string
	}{
		{adaptor.ErrNotFound{Name: "notfound"}, "adaptor 'notfound' not found in registry"},
		{adaptor.ErrFuncNotSupported{Name: "unsupported", Func: "Client()"}, "'unsupported' does not support 'Client()' function"},
	}
)

func TestErrors(t *testing.T) {
	for _, et := range errorTests {
		if !reflect.DeepEqual(et.e.Error(), et.expected) {
			t.Errorf("wrong Error(), expected %s, got %s", et.expected, et.e.Error())
		}
	}
}
