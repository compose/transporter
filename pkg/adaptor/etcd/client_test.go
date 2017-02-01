package etcd

import (
	"reflect"
	"testing"
	"time"

	t_err "github.com/compose/transporter/pkg/error"
	eclient "github.com/coreos/etcd/client"
)

var defaultClient = &Client{
	cfg: eclient.Config{
		Endpoints:               DefaultEndpoints,
		HeaderTimeoutPerRequest: DefaultRequestTimeout,
		Transport:               eclient.DefaultTransport,
	},
}

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
		"with_url_fake",
		[]ClientOptionFunc{WithURI("http://fakeurl:5432")},
		&Client{
			cfg: eclient.Config{
				Endpoints:               []string{"http://fakeurl:5432"},
				HeaderTimeoutPerRequest: DefaultRequestTimeout,
				Transport:               eclient.DefaultTransport,
			},
		},
		nil,
	},
	{
		"with_auth_url",
		[]ClientOptionFunc{WithURI("http://user:pwd@authedurl:5432")},
		&Client{
			cfg: eclient.Config{
				Endpoints:               []string{"http://authedurl:5432"},
				Username:                "user",
				Password:                "pwd",
				HeaderTimeoutPerRequest: DefaultRequestTimeout,
				Transport:               eclient.DefaultTransport,
			},
		},
		nil,
	},
	{
		"with_uri_invalid",
		[]ClientOptionFunc{WithURI("://user@pass:5432")},
		&Client{},
		t_err.InvalidURIError{URI: "://user@pass:5432", ErrDetail: "parse ://user@pass:5432: missing protocol scheme"},
	},
	{
		"with_timeout",
		[]ClientOptionFunc{WithTimeout("30s")},
		&Client{
			cfg: eclient.Config{
				Endpoints:               DefaultEndpoints,
				HeaderTimeoutPerRequest: 30 * time.Second,
				Transport:               eclient.DefaultTransport,
			},
		},
		nil,
	},
	{
		"with_timeout_empty",
		[]ClientOptionFunc{WithTimeout("")},
		defaultClient,
		nil,
	},
	{
		"with_timeout_invalid",
		[]ClientOptionFunc{WithTimeout("blah")},
		&Client{},
		t_err.InvalidTimeoutError{Timeout: "blah"},
	},
}

func TestClient(t *testing.T) {
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

var expectedConnectErr = t_err.ConnectError{Reason: "request to get version failed"}

func TestFailedConnect(t *testing.T) {
	c, _ := NewClient(WithURI("http://127.0.0.1:5432"))
	_, err := c.Connect()
	if err != expectedConnectErr {
		t.Errorf("wrong Connect error\n%+v\n%+v", expectedConnectErr, err)
	}
}
