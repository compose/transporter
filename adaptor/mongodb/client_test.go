package mongodb

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/client"

	mgo "gopkg.in/mgo.v2"
)

var errorTests = []struct {
	name     string
	expected string
	e        error
}{
	{
		"OplogAccessError",
		"oplog access failed, database missing oplog.rs collection",
		OplogAccessError{"database missing oplog.rs collection"},
	},
	{
		"InvalidReadPreferenceError",
		"Invalid Read Preference, fakeReadPreference",
		InvalidReadPreferenceError{"fakeReadPreference"},
	},
}

func TestErrors(t *testing.T) {
	for _, et := range errorTests {
		if et.e.Error() != et.expected {
			t.Errorf("[%s] wrong Error(), expected %s, got %s", et.name, et.expected, et.e.Error())
		}
	}
}

const rootPEM = `
-----BEGIN CERTIFICATE-----
MIIEBDCCAuygAwIBAgIDAjppMA0GCSqGSIb3DQEBBQUAMEIxCzAJBgNVBAYTAlVT
MRYwFAYDVQQKEw1HZW9UcnVzdCBJbmMuMRswGQYDVQQDExJHZW9UcnVzdCBHbG9i
YWwgQ0EwHhcNMTMwNDA1MTUxNTU1WhcNMTUwNDA0MTUxNTU1WjBJMQswCQYDVQQG
EwJVUzETMBEGA1UEChMKR29vZ2xlIEluYzElMCMGA1UEAxMcR29vZ2xlIEludGVy
bmV0IEF1dGhvcml0eSBHMjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
AJwqBHdc2FCROgajguDYUEi8iT/xGXAaiEZ+4I/F8YnOIe5a/mENtzJEiaB0C1NP
VaTOgmKV7utZX8bhBYASxF6UP7xbSDj0U/ck5vuR6RXEz/RTDfRK/J9U3n2+oGtv
h8DQUB8oMANA2ghzUWx//zo8pzcGjr1LEQTrfSTe5vn8MXH7lNVg8y5Kr0LSy+rE
ahqyzFPdFUuLH8gZYR/Nnag+YyuENWllhMgZxUYi+FOVvuOAShDGKuy6lyARxzmZ
EASg8GF6lSWMTlJ14rbtCMoU/M4iarNOz0YDl5cDfsCx3nuvRTPPuj5xt970JSXC
DTWJnZ37DhF5iR43xa+OcmkCAwEAAaOB+zCB+DAfBgNVHSMEGDAWgBTAephojYn7
qwVkDBF9qn1luMrMTjAdBgNVHQ4EFgQUSt0GFhu89mi1dvWBtrtiGrpagS8wEgYD
VR0TAQH/BAgwBgEB/wIBADAOBgNVHQ8BAf8EBAMCAQYwOgYDVR0fBDMwMTAvoC2g
K4YpaHR0cDovL2NybC5nZW90cnVzdC5jb20vY3Jscy9ndGdsb2JhbC5jcmwwPQYI
KwYBBQUHAQEEMTAvMC0GCCsGAQUFBzABhiFodHRwOi8vZ3RnbG9iYWwtb2NzcC5n
ZW90cnVzdC5jb20wFwYDVR0gBBAwDjAMBgorBgEEAdZ5AgUBMA0GCSqGSIb3DQEB
BQUAA4IBAQA21waAESetKhSbOHezI6B1WLuxfoNCunLaHtiONgaX4PCVOzf9G0JY
/iLIa704XtE7JW4S615ndkZAkNoUyHgN7ZVm2o6Gb4ChulYylYbc3GrKBIxbf/a/
zG+FA1jDaFETzf3I93k9mTXwVqO94FntT0QJo544evZG0R0SnU++0ED8Vf4GXjza
HFa9llF7b1cq26KqltyMdMKVvvBulRP/F/A8rLIQjcxz++iPAsbw+zOzlTvjwsto
WHPbqCRiOwY1nQ2pM714A5AuTHhdUDqB1O6gyHA43LL5Z/qHQF1hwFGPa4NrzQU6
yuGnBXj8ytqU0CwIPX4WecigUCAkVDNx
-----END CERTIFICATE-----`

var (
	defaultClient = &Client{
		uri:            DefaultURI,
		sessionTimeout: DefaultSessionTimeout,
		safety:         DefaultSafety,
		readPreference: DefaultReadPreference,
	}

	certPool = func() *x509.CertPool {
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM([]byte(rootPEM))
		return pool
	}
)

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
		[]ClientOptionFunc{WithURI("mongodb://fakeurl:27017")},
		&Client{
			uri:            "mongodb://fakeurl:27017",
			sessionTimeout: DefaultSessionTimeout,
			safety:         DefaultSafety,
			readPreference: DefaultReadPreference,
		},
		nil,
	},
	{
		"with_uri_invalid",
		[]ClientOptionFunc{WithURI("mongodb://@nopassword:27017")},
		&Client{},
		client.InvalidURIError{URI: "mongodb://@nopassword:27017", Err: "credentials must be provided as user:pass@host"},
	},
	{
		"with_timeout",
		[]ClientOptionFunc{WithTimeout("30s")},
		&Client{
			uri:            DefaultURI,
			sessionTimeout: 30 * time.Second,
			safety:         DefaultSafety,
			readPreference: DefaultReadPreference,

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
		client.InvalidTimeoutError{Timeout: "blah"},
	},
	{
		"with_write_concern",
		[]ClientOptionFunc{WithWriteConcern(2)},
		&Client{
			uri:            DefaultURI,
			sessionTimeout: DefaultSessionTimeout,
			safety:         mgo.Safe{W: 2},
			readPreference: DefaultReadPreference,
		},
		nil,
	},
	{
		"with_fsync",
		[]ClientOptionFunc{WithFsync(true)},
		&Client{
			uri:            DefaultURI,
			sessionTimeout: DefaultSessionTimeout,
			safety:         mgo.Safe{FSync: true},
			readPreference: DefaultReadPreference,
		},
		nil,
	},
	{
		"with_tail",
		[]ClientOptionFunc{WithTail(true)},
		&Client{
			uri:            DefaultURI,
			sessionTimeout: DefaultSessionTimeout,
			safety:         DefaultSafety,
			tail:           true,
			readPreference: DefaultReadPreference,
		},
		nil,
	},
	{
		"with_ssl",
		[]ClientOptionFunc{WithSSL(true)},
		&Client{
			uri:            DefaultURI,
			sessionTimeout: DefaultSessionTimeout,
			safety:         DefaultSafety,
			tlsConfig:      &tls.Config{InsecureSkipVerify: true, RootCAs: x509.NewCertPool()},
			readPreference: DefaultReadPreference,
		},
		nil,
	},
	{
		"with_ssl_with_certs",
		[]ClientOptionFunc{WithSSL(true), WithCACerts([]string{rootPEM})},
		&Client{
			uri:            DefaultURI,
			sessionTimeout: DefaultSessionTimeout,
			safety:         DefaultSafety,
			tlsConfig:      &tls.Config{InsecureSkipVerify: false, RootCAs: certPool()},
			readPreference: DefaultReadPreference,
		},
		nil,
	},
	{
		"with_ssl_with_cert_file",
		[]ClientOptionFunc{WithSSL(true), WithCACerts([]string{"testdata/ca.pem"})},
		&Client{
			uri:            DefaultURI,
			sessionTimeout: DefaultSessionTimeout,
			safety:         DefaultSafety,
			tlsConfig:      &tls.Config{InsecureSkipVerify: false, RootCAs: certPool()},
			readPreference: DefaultReadPreference,
		},
		nil,
	},
	{
		"with_ssl_with_cert_file_permission_denied",
		[]ClientOptionFunc{WithSSL(true), WithCACerts([]string{"testdata/ca_no_perms.pem"})},
		&Client{
			uri:            DefaultURI,
			sessionTimeout: DefaultSessionTimeout,
			safety:         DefaultSafety,
			tlsConfig:      &tls.Config{InsecureSkipVerify: false, RootCAs: certPool()},
			readPreference: DefaultReadPreference,
		},
		&os.PathError{Op: "open", Path: "testdata/ca_no_perms.pem", Err: os.ErrPermission},
	},
	{
		"with_certs",
		[]ClientOptionFunc{WithCACerts([]string{rootPEM})},
		&Client{
			uri:            DefaultURI,
			sessionTimeout: DefaultSessionTimeout,
			safety:         DefaultSafety,
			tlsConfig:      &tls.Config{InsecureSkipVerify: false, RootCAs: certPool()},
			readPreference: DefaultReadPreference,
		},
		nil,
	},
	{
		"with_certs_invalid",
		[]ClientOptionFunc{WithCACerts([]string{"notacert"})},
		&Client{
			uri:            DefaultURI,
			sessionTimeout: DefaultSessionTimeout,
			safety:         DefaultSafety,
			tlsConfig:      &tls.Config{InsecureSkipVerify: false, RootCAs: certPool()},
			readPreference: DefaultReadPreference,
		},
		client.ErrInvalidCert,
	},
	{
		"with_read_preference_invalid",
		[]ClientOptionFunc{WithReadPreference("blah")},
		&Client{},
		InvalidReadPreferenceError{ReadPreference: "blah"},
	},
	{
		"with_primary_read_preference",
		[]ClientOptionFunc{WithReadPreference("Primary")},
		&Client{
			uri:            DefaultURI,
			sessionTimeout: DefaultSessionTimeout,
			readPreference: 2,
		},
		nil,
	},
	{
		"with_primary_preferred_read_preference_valid",
		[]ClientOptionFunc{WithReadPreference("PrimaryPreferred")},
		&Client{
			uri:            DefaultURI,
			sessionTimeout: DefaultSessionTimeout,
			readPreference: 3,
		},
		nil,
	},
	{
		"with_secondary_read_preference_valid",
		[]ClientOptionFunc{WithReadPreference("Secondary")},
		&Client{
			uri:            DefaultURI,
			sessionTimeout: DefaultSessionTimeout,
			readPreference: 4,
		},
		nil,
	},
	{
		"with_secondary_preferred_read_preference_valid",
		[]ClientOptionFunc{WithReadPreference("SecondaryPreferred")},
		&Client{
			uri:            DefaultURI,
			sessionTimeout: DefaultSessionTimeout,
			readPreference: 5,
		},
		nil,
	},
	{
		"with_nearest_read_preference_valid",
		[]ClientOptionFunc{WithReadPreference("Nearest")},
		&Client{
			uri:            DefaultURI,
			sessionTimeout: DefaultSessionTimeout,
			readPreference: 6,
		},
		nil,
	},
	{
		"with_default_read_preference",
		[]ClientOptionFunc{WithReadPreference("")},
		&Client{
			uri:            DefaultURI,
			sessionTimeout: DefaultSessionTimeout,
			readPreference: 2,
		},
		nil,
	},
}

func TestNewClient(t *testing.T) {
	os.Chmod("testdata/ca_no_perms.pem", 0222)
	defer os.Chmod("testdata/ca_no_perms.pem", 0644)
	for _, ct := range clientTests {
		actual, err := NewClient(ct.options...)
		if ct.expectedErr != nil && !reflect.DeepEqual(err.Error(), ct.expectedErr.Error()) {
			t.Fatalf("[%s] unexpected NewClient error, expected %+v, got %+v\n", ct.name, ct.expectedErr, err)
		}
		if err == nil && !reflect.DeepEqual(ct.expected, actual) {
			t.Errorf("[%s] Client mismatch\nexpected %+v\ngot %+v", ct.name, ct.expected, actual)
		}
	}
}

var (
	caCertPool = func() *x509.CertPool {
		pool := x509.NewCertPool()
		c, _ := ioutil.ReadFile("/tmp/mongodb/ca.crt")
		pool.AppendCertsFromPEM(c)
		return pool
	}

	connectTests = []struct {
		name        string
		client      *Client
		expectedErr error
	}{
		{
			"default connect",
			&Client{
				uri:            DefaultURI,
				sessionTimeout: DefaultSessionTimeout,
				safety:         DefaultSafety,
			},
			nil,
		},
		{
			"timeout connect",
			&Client{
				uri:            "mongodb://localhost:37017",
				sessionTimeout: 2 * time.Second,
				safety:         DefaultSafety,
			},
			client.ConnectError{Reason: "no reachable servers"},
		},
		{
			"authenticated connect",
			&Client{
				uri:            "mongodb://transporter:transporter@127.0.0.1:10000,127.0.0.1:10001/admin",
				sessionTimeout: DefaultSessionTimeout,
				safety:         DefaultSafety,
			},
			nil,
		},
		{
			"failed authenticated connect",
			&Client{
				uri:            "mongodb://transporter:wrongpassword@127.0.0.1:10000,127.0.0.1:10001/admin",
				sessionTimeout: DefaultSessionTimeout,
				safety:         DefaultSafety,
			},
			client.ConnectError{Reason: "server returned error on SASL authentication step: Authentication failed."},
		},
		{
			"connect with ssl and verify",
			&Client{
				uri:            "mongodb://localhost:11112/test",
				sessionTimeout: DefaultSessionTimeout,
				safety:         DefaultSafety,
				tlsConfig:      &tls.Config{InsecureSkipVerify: false, RootCAs: caCertPool()},
			},
			nil,
		},
		{
			"connect with ssl skip verify",
			&Client{
				uri:            "mongodb://localhost:11112/test",
				sessionTimeout: DefaultSessionTimeout,
				safety:         DefaultSafety,
				tlsConfig:      &tls.Config{InsecureSkipVerify: true, RootCAs: x509.NewCertPool()},
			},
			nil,
		},
		{
			"with_tail",
			&Client{
				uri:            DefaultURI,
				sessionTimeout: DefaultSessionTimeout,
				safety:         DefaultSafety,
				tail:           true,
			},
			nil,
		},
		{
			"with tail not replset",
			&Client{
				uri:            "mongodb://127.0.0.1:29017",
				sessionTimeout: DefaultSessionTimeout,
				safety:         DefaultSafety,
				tail:           true,
			},
			OplogAccessError{"database missing oplog.rs collection"},
		},
		{
			"with tail no access",
			&Client{
				uri:            "mongodb://list_but_cant_read:xyz123@127.0.0.1:10000,127.0.0.1:10001/test",
				sessionTimeout: DefaultSessionTimeout,
				safety:         DefaultSafety,
				tail:           true,
			},
			OplogAccessError{"not authorized for oplog.rs collection"},
		},
		{
			"with tail no privileges",
			&Client{
				uri:            "mongodb://cant_read:limited1234@127.0.0.1:10000,127.0.0.1:10001/test",
				sessionTimeout: DefaultSessionTimeout,
				safety:         DefaultSafety,
				tail:           true,
			},
			OplogAccessError{"unable to list collections on local database"},
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
