package rethinkdb

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"reflect"
	"testing"
	"time"
)

var errorTests = []struct {
	name     string
	expected string
	e        error
}{
	{
		"InvalidURIError",
		"Invalid URI (blah), blah",
		InvalidURIError{"blah", "blah"},
	},
	{
		"InvalidTimeoutError",
		"Invalid Timeout, 10",
		InvalidTimeoutError{"10"},
	},
	{
		"InvalidCertError",
		"failed to parse root certificate",
		InvalidCertError{true},
	},
	{
		"InvalidCertError with version",
		"invalid cert error",
		InvalidCertError{},
	},
	{
		"ConnectError with timeout",
		"connection error, no reachable servers",
		ConnectError{"no reachable servers"},
	},
	{
		"VersionError",
		"unable to determine version from rethinkdb://localhost:28105, its bad",
		VersionError{"rethinkdb://localhost:28105", "", "its bad"},
	},
	{
		"VersionError with version",
		"rethinkdb://localhost:28105 running 0.9.2, its bad",
		VersionError{"rethinkdb://localhost:28105", "0.9.2", "its bad"},
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
		db:             DefaultDatabase,
		sessionTimeout: DefaultTimeout,
		tlsConfig:      nil,
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
		[]ClientOptionFunc{WithURI("rethinkdb://fakeurl:28015")},
		&Client{
			uri:            "rethinkdb://fakeurl:28015",
			db:             DefaultDatabase,
			sessionTimeout: DefaultTimeout,
		},
		nil,
	},
	{
		"with_database",
		[]ClientOptionFunc{WithDatabase("not_the_default")},
		&Client{
			uri:            DefaultURI,
			db:             "not_the_default",
			sessionTimeout: DefaultTimeout,
		},
		nil,
	},
	{
		"with_database_empty",
		[]ClientOptionFunc{WithDatabase("")},
		&Client{
			uri:            DefaultURI,
			db:             DefaultDatabase,
			sessionTimeout: DefaultTimeout,
		},
		nil,
	},
	{
		"with_timeout",
		[]ClientOptionFunc{WithSessionTimeout("30s")},
		&Client{
			uri:            DefaultURI,
			db:             DefaultDatabase,
			sessionTimeout: 30 * time.Second,
		},
		nil,
	},
	{
		"with_timeout_empty",
		[]ClientOptionFunc{WithSessionTimeout("")},
		defaultClient,
		nil,
	},
	{
		"with_timeout_invalid",
		[]ClientOptionFunc{WithSessionTimeout("blah")},
		&Client{},
		InvalidTimeoutError{"blah"},
	},
	{
		"with_ssl",
		[]ClientOptionFunc{WithSSL(true)},
		&Client{
			uri:            DefaultURI,
			db:             DefaultDatabase,
			sessionTimeout: DefaultTimeout,
			tlsConfig:      &tls.Config{InsecureSkipVerify: true, RootCAs: x509.NewCertPool()},
		},
		nil,
	},
	{
		"with_ssl_with_certs",
		[]ClientOptionFunc{WithSSL(true), WithCACerts([]string{rootPEM})},
		&Client{
			uri:            DefaultURI,
			db:             DefaultDatabase,
			sessionTimeout: DefaultTimeout,
			tlsConfig:      &tls.Config{InsecureSkipVerify: false, RootCAs: certPool()},
		},
		nil,
	},
	{
		"with_certs",
		[]ClientOptionFunc{WithCACerts([]string{rootPEM})},
		&Client{
			uri:            DefaultURI,
			db:             DefaultDatabase,
			sessionTimeout: DefaultTimeout,
			tlsConfig:      &tls.Config{InsecureSkipVerify: false, RootCAs: certPool()},
		},
		nil,
	},
	{
		"with_certs_invalid",
		[]ClientOptionFunc{WithCACerts([]string{"notacert"})},
		&Client{
			uri:            DefaultURI,
			db:             DefaultDatabase,
			sessionTimeout: DefaultTimeout,
			tlsConfig:      &tls.Config{InsecureSkipVerify: false, RootCAs: certPool()},
		},
		InvalidCertError{true},
	},
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
	caCertPool = func() *x509.CertPool {
		pool := x509.NewCertPool()
		c, _ := ioutil.ReadFile("/tmp/rethinkdb/ca.crt")
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
				db:             DefaultDatabase,
				sessionTimeout: DefaultTimeout,
			},
			nil,
		},
		{
			"timeout connect",
			&Client{
				uri:            "rethinkdb://127.0.0.1:37017",
				sessionTimeout: 2 * time.Second,
			},
			ConnectError{"gorethink: dial tcp 127.0.0.1:37017: getsockopt: connection refused"},
		},
		{
			"authenticated connect",
			&Client{
				uri:            "rethinkdb://admin:admin123@127.0.0.1:48015",
				db:             DefaultDatabase,
				sessionTimeout: DefaultTimeout,
			},
			nil,
		},
		{
			"failed authenticated connect",
			&Client{
				uri:            "rethinkdb://admin:wrongpassword@127.0.0.1:48015",
				db:             DefaultDatabase,
				sessionTimeout: DefaultTimeout,
			},
			ConnectError{"gorethink: Wrong password"},
		},
		{
			"connect with ssl and verify",
			&Client{
				uri:            "rethinkdb://localhost:38015",
				db:             DefaultDatabase,
				sessionTimeout: DefaultTimeout,
				tlsConfig:      &tls.Config{InsecureSkipVerify: false, RootCAs: caCertPool()},
			},
			nil,
		},
		{
			"connect with ssl skip verify",
			&Client{
				uri:            "rethinkdb://localhost:38015",
				db:             DefaultDatabase,
				sessionTimeout: DefaultTimeout,
				tlsConfig:      &tls.Config{InsecureSkipVerify: true, RootCAs: x509.NewCertPool()},
			},
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
		if err != nil {
			ct.client.Close()
		}
	}
}
