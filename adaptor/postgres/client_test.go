package postgres

import (
	"errors"
	"reflect"
	"testing"
)

var (
	defaultClient = &Client{
		uri: DefaultURI,
		db:  "postgres",
	}

	errBadClient = errors.New("bad client")

	clientTests = []struct {
		name        string
		options     []ClientOptionFunc // input
		expected    *Client            // expected result
		expectedErr error              // expected error
	}{
		{
			"default_client",
			make([]ClientOptionFunc, 0),
			defaultClient,
			nil,
		},
		{
			"with_err",
			[]ClientOptionFunc{WithErr()},
			defaultClient,
			errBadClient,
		},
	}
)

func WithErr() ClientOptionFunc {
	return func(c *Client) error {
		return errBadClient
	}
}

func TestNewClient(t *testing.T) {
	for _, ct := range clientTests {
		actual, err := NewClient(ct.options...)
		if err != ct.expectedErr {
			t.Fatalf("[%s] unexpected NewClient error, expected %+v, got %+v\n", ct.name, ct.expectedErr, err)
		}
		if err == nil && !reflect.DeepEqual(ct.expected, actual) {
			t.Errorf("[%s] Client mismatch\nexpected %+v\ngot %+v", ct.name, ct.expected, actual)
		}
	}
}

var (
	connectTests = []struct {
		name        string
		client      *Client
		expectedErr error
	}{
		{
			"default connect",
			defaultClient,
			nil,
		},
	}
)

func TestConnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Connect in short mode")
	}

	for _, ct := range connectTests {
		_, err := ct.client.Connect()
		if err != ct.expectedErr {
			t.Fatalf("[%s] unexpected Connect error, expected %+v, got %+v\n", ct.name, ct.expectedErr, err)
		}
		if err == nil {
			ct.client.Close()
		}
	}
}
