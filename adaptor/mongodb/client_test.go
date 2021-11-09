package mongodb

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/client"
	"gopkg.in/mgo.v2"
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

func CompareTlsConfig(t *testing.T, testName string, config1 *tls.Config, config2 *tls.Config) {
	if config1 != nil && config1.InsecureSkipVerify != config2.InsecureSkipVerify {
		t.Errorf(
			"[%s] TLS Config mismatch on InsecureSkipVerify.\nexpected %+v\ngot %+v",
			testName,
			config1.InsecureSkipVerify,
			config2.InsecureSkipVerify,
		)
	}

	if config1 != nil && config1.RootCAs != nil && !reflect.DeepEqual(config1.RootCAs.Subjects(), config2.RootCAs.Subjects()) {
		t.Errorf(
			"[%s] TLS Config mismatch on RootCAs.\nexpected %+v\ngot %+v",
			testName,
			config1.RootCAs,
			config2.RootCAs,
		)
	}
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
MIIDwTCCAqmgAwIBAgIUc5GVZpeyfa2o39cu125J+pajttswDQYJKoZIhvcNAQEL
BQAwYTELMAkGA1UEBhMCQVUxDDAKBgNVBAgMA05TVzEVMBMGA1UECgwMT3JnYW5p
c2F0aW9uMQ0wCwYDVQQDDARyb290MR4wHAYJKoZIhvcNAQkBFg91c2VyQGRvbWFp
bi5jb20wHhcNMjExMTAzMTkzMjA1WhcNMzExMTAxMTkzMjA1WjBhMQswCQYDVQQG
EwJBVTEMMAoGA1UECAwDTlNXMRUwEwYDVQQKDAxPcmdhbmlzYXRpb24xDTALBgNV
BAMMBHJvb3QxHjAcBgkqhkiG9w0BCQEWD3VzZXJAZG9tYWluLmNvbTCCASIwDQYJ
KoZIhvcNAQEBBQADggEPADCCAQoCggEBAKNuzkIsBEeCHvMRbABCDdv3Gxf2Wku1
Ne6zolyIBc4Ueafv1aHppnkD4AZT6Wof+jNYLBFMyT2dHKPQkCufrZifoyC05m0z
/K5I83VFmiCsg+cXMny327FtkYvF0w7R0kFVMwnZlk/GaNXP8CGusfc8WLY/M4/+
GcGfyt21TJaF9thUKZTtOt6UWVAxeq08l+r1bwyul/Mgr+CnvNWEFV5i9TXCOhRD
9U6PLgIij3GPcv9Ons2uORb4SGHXOuPUKFEMTVxmiqKPXNjLyVuhsMuqWrGUm1wH
jPGcWhQeOv579S+9GXTrxZcrIO9qe95tyUhizPBPQXO+ob17/GdhPiECAwEAAaNx
MG8wHQYDVR0OBBYEFB+sQecPXUvKYZ5ezJ5LSfzQGXKpMB8GA1UdIwQYMBaAFB+s
QecPXUvKYZ5ezJ5LSfzQGXKpMA8GA1UdEwEB/wQFMAMBAf8wHAYDVR0RBBUwE4IR
dHJhbnNwb3J0ZXItbW9uZ28wDQYJKoZIhvcNAQELBQADggEBAGkgTlHDDwBV45tX
PZrVlQzFG3j/kcbcDCP/lvU7lMA2bpk2Ovj5dOfSuO0uiIvLFyuvrOaKKU/56Wwb
hAwhcJ4lKL7G8SYtyqnlkdvjjXST4yrqHmUtFFx+oPWvN/G2phpvUyxE3IyqlRd9
edx/Yq2zrFXzAvH30WsZ1ZjeFrDEh5oDmRTvx9qjacLSsNRvjwbp87nSLTuppix+
VaQpgVeuGloO/uwUjhkztujS8zVSN4jREgrU3cpi/Sd0z2gGF8GRizZgyPIWRIzl
qsCk4QqkxWYWblt4H1m6RmVZuXkKTNA2X6Xc/idnV9wyTdzqK8xwy118M2o2MwJ5
49igC1w=
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
		c, err := ioutil.ReadFile("testdata/ca.pem")
		if err != nil {
			log.Fatal(err)
		}
		pool.AppendCertsFromPEM(c)
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
		"with_certs_not_found",
		[]ClientOptionFunc{WithCACerts([]string{"thisfiledoesnotexist"})},
		&Client{
			uri:            DefaultURI,
			sessionTimeout: DefaultSessionTimeout,
			safety:         DefaultSafety,
			tlsConfig:      &tls.Config{InsecureSkipVerify: false, RootCAs: certPool()},
			readPreference: DefaultReadPreference,
		},
		errors.New("Cert file not found"),
	},
	{
		"with_certs_invalid",
		[]ClientOptionFunc{WithCACerts([]string{"testdata/ca_invalid.pem"})},
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

		if err == nil {
			// Can't properly compare tls.config when there's a RootCA set
			expectedTlsConfig := ct.expected.tlsConfig
			actualTlsConfig := actual.tlsConfig

			ct.expected.tlsConfig = nil
			actual.tlsConfig = nil

			if !reflect.DeepEqual(ct.expected, actual) {
				t.Errorf("[%s] Client mismatch\nexpected %+v\ngot %+v", ct.name, ct.expected, actual)
			}

			if expectedTlsConfig != nil {
				CompareTlsConfig(t, ct.name, expectedTlsConfig, actualTlsConfig)
			}
		}
	}
}

var (
	// Not needed as long as we can't make "connect with ssl and verify" test case pass
	// caCertPool = func() *x509.CertPool {
	// 	pool := x509.NewCertPool()
	// 	c, err := ioutil.ReadFile("testdata/ca.pem")
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	pool.AppendCertsFromPEM(c)
	// 	return pool
	// }

	// clientCerts = func() []tls.Certificate {
	// 	clientCerts := []tls.Certificate{}
	// 	cert, err := tls.LoadX509KeyPair("testdata/client.crt", "testdata/client.key")
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}

	// 	clientCerts = append(clientCerts, cert)

	// 	return clientCerts
	// }

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
				uri:            "mongodb://transporter-db:37017",
				sessionTimeout: 2 * time.Second,
				safety:         DefaultSafety,
			},
			client.ConnectError{Reason: "no reachable servers"},
		},
		{
			"authenticated connect",
			&Client{
				uri:            "mongodb://transporter:transporter@transporter-db:10000,transporter-db:10001/admin",
				sessionTimeout: DefaultSessionTimeout,
				safety:         DefaultSafety,
			},
			nil,
		},
		{
			"failed authenticated connect",
			&Client{
				uri:            "mongodb://transporter:wrongpassword@transporter-db:10000,transporter-db:10001/admin",
				sessionTimeout: DefaultSessionTimeout,
				safety:         DefaultSafety,
			},
			client.ConnectError{Reason: "server returned error on SASL authentication step: Authentication failed."},
		},
		// Deactivated, can't make it work with mgo driver. Will need to try again with the official mongodb driver.
		// {
		// 	"connect with ssl and verify",
		// 	&Client{
		// 		uri:            "mongodb://transporter-db:11112/test",
		// 		sessionTimeout: DefaultSessionTimeout,
		// 		safety:         DefaultSafety,
		// 		tlsConfig:      &tls.Config{InsecureSkipVerify: false, RootCAs: caCertPool(), Certificates: clientCerts()},
		// 	},
		// 	nil,
		// },
		{
			"connect with ssl skip verify",
			&Client{
				uri:            "mongodb://transporter-db:11112/test",
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
				uri:            "mongodb://transporter-db:29017",
				sessionTimeout: DefaultSessionTimeout,
				safety:         DefaultSafety,
				tail:           true,
			},
			OplogAccessError{"database missing oplog.rs collection"},
		},
		{
			"with tail no access",
			&Client{
				uri:            "mongodb://list_but_cant_read:xyz123@transporter-db:10000,transporter-db:10001/test",
				sessionTimeout: DefaultSessionTimeout,
				safety:         DefaultSafety,
				tail:           true,
			},
			OplogAccessError{"not authorized for oplog.rs collection"},
		},
		{
			"with tail no privileges",
			&Client{
				uri:            "mongodb://cant_read:limited1234@transporter-db:10000,transporter-db:10001/test",
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
