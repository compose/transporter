package client_test

import (
	"testing"

	"github.com/compose/transporter/client"
)

var errorTests = []struct {
	name     string
	expected string
	e        error
}{
	{
		"InvalidURIError",
		"Invalid URI (blah), blah",
		client.InvalidURIError{URI: "blah", Err: "blah"},
	},
	{
		"InvalidTimeoutError",
		"Invalid Timeout, 10",
		client.InvalidTimeoutError{Timeout: "10"},
	},
	{
		"ConnectError with timeout",
		"connection error, no reachable servers",
		client.ConnectError{Reason: "no reachable servers"},
	},
	{
		"VersionError",
		"unable to determine version from rethinkdb://localhost:28105, its bad",
		client.VersionError{URI: "rethinkdb://localhost:28105", V: "", Err: "its bad"},
	},
	{
		"VersionError with version",
		"rethinkdb://localhost:28105 running 0.9.2, its bad",
		client.VersionError{URI: "rethinkdb://localhost:28105", V: "0.9.2", Err: "its bad"},
	},
}

func TestErrors(t *testing.T) {
	for _, et := range errorTests {
		if et.e.Error() != et.expected {
			t.Errorf("[%s] wrong Error(), expected %s, got %s", et.name, et.expected, et.e.Error())
		}
	}
}
