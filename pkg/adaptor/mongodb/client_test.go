package mongodb

import (
	"crypto/tls"
	"crypto/x509"
	"reflect"
	"testing"
	"time"

	mgo "gopkg.in/mgo.v2"
)

var (
	defaultClient = &Client{
		uri:            DefaultURI,
		sessionTimeout: DefaultSessionTimeout,
		safety:         DefaultSafety,
	}
)

func TestNewClient(t *testing.T) {
	var clientTests = []struct {
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
			"fakeurl",
			[]ClientOptionFunc{WithURI("mongodb://fakeurl:27017")},
			&Client{
				uri:            "mongodb://fakeurl:27017",
				sessionTimeout: DefaultSessionTimeout,
				safety:         DefaultSafety,
			},
			nil,
		},
		{
			"invalid_uri",
			[]ClientOptionFunc{WithURI("mongodb://@nopassword:27017")},
			&Client{},
			InvalidURIError{"mongodb://@nopassword:27017", "credentials must be provided as user:pass@host"},
		},
		{
			"custom_timeout",
			[]ClientOptionFunc{WithTimeout("30s")},
			&Client{
				uri:            DefaultURI,
				sessionTimeout: 30 * time.Second,
				safety:         DefaultSafety,
			},
			nil,
		},
		{
			"empty_timeout",
			[]ClientOptionFunc{WithTimeout("")},
			defaultClient,
			nil,
		},
		{
			"invalid_timeout",
			[]ClientOptionFunc{WithTimeout("blah")},
			&Client{},
			InvalidTimeoutError{"blah"},
		},
		{
			"custom_write_concern",
			[]ClientOptionFunc{WithWriteConcern(2)},
			&Client{
				uri:            DefaultURI,
				sessionTimeout: DefaultSessionTimeout,
				safety:         mgo.Safe{W: 2},
			},
			nil,
		},
		{
			"custom_fsync",
			[]ClientOptionFunc{WithFsync(true)},
			&Client{
				uri:            DefaultURI,
				sessionTimeout: DefaultSessionTimeout,
				safety:         mgo.Safe{FSync: true},
			},
			nil,
		},
		{
			"set_tail",
			[]ClientOptionFunc{WithTail(true)},
			&Client{
				uri:            DefaultURI,
				sessionTimeout: DefaultSessionTimeout,
				safety:         DefaultSafety,
				tail:           true,
			},
			nil,
		},
		{
			"set_ssl",
			[]ClientOptionFunc{WithSSL(true)},
			&Client{
				uri:            DefaultURI,
				sessionTimeout: DefaultSessionTimeout,
				safety:         DefaultSafety,
				tlsConfig:      &tls.Config{InsecureSkipVerify: true, RootCAs: x509.NewCertPool()},
			},
			nil,
		},
	}
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

func TestConnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Connect in short mode")
	}
	c, err := NewClient()
	if err != nil {
		t.Fatalf("unexpected NewClient error, %s\n", err)
	}
	_, err = c.Connect()
	if err != nil {
		t.Errorf("unexpected Connect error, %s\n", err)
	}
}

func TestConnectFail(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Connect in short mode")
	}
	c, err := NewClient(WithURI("mongodb://localhost:27018"), WithTimeout("2s"))
	if err != nil {
		t.Fatalf("unexpected NewClient error, %s\n", err)
	}
	_, err = c.Connect()
	if err == nil {
		t.Fatal("no error return but expected one")
	}
}
