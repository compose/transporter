package gojajs

import (
	"errors"
	"reflect"
	"testing"
)

var (
	defaultClient = &Client{}

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
			"default_client_with_filename",
			[]ClientOptionFunc{WithFilename("testdata/transformer.js")},
			&Client{fn: `function transform(doc) { return doc }`},
			nil,
		},
		{
			"default_client_empty_filename",
			[]ClientOptionFunc{WithFilename("")},
			nil,
			ErrEmptyFilename,
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
