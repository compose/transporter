package file

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/compose/transporter/pkg/log"
)

var (
	defaultClient = &Client{
		uri: DefaultURI,
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
			"with_uri_fake",
			[]ClientOptionFunc{WithURI("/path/to/a/file.out")},
			&Client{
				uri: "/path/to/a/file.out",
			},
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
		cleanupFunc func(dir string)
	}{
		{
			"default connect",
			defaultClient,
			nil,
			func(dir string) {},
		},
		{
			"default connect",
			&Client{
				uri: fmt.Sprintf("file://%s/file_test.out", testTmpDir("connect_test")),
			},
			nil,
			func(dir string) {
				log.Infof("removing dir: %s", dir)
				os.RemoveAll(dir)
			},
		},
	}
)

func TestConnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Connect in short mode")
	}
	ioutil.TempDir("", "prefix")
	for _, ct := range connectTests {
		_, err := ct.client.Connect()
		if err != ct.expectedErr {
			t.Fatalf("[%s] unexpected Connect error, expected %+v, got %+v\n", ct.name, ct.expectedErr, err)
		}
		if err == nil {
			ct.client.Close()
		}
		tDir := strings.Replace(ct.client.uri, "file://", "", 1)
		tDir = strings.Replace(tDir, "file_test.out", "", 1)
		ct.cleanupFunc(tDir)
	}
}
