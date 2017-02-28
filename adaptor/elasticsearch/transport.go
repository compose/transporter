package elasticsearch

import (
	"net/http"

	awsauth "github.com/smartystreets/go-aws-auth"
)

// AWSTransport handles wrapping requests to AWS Elasticsearch service
type AWSTransport struct {
	Credentials awsauth.Credentials
	transport   http.RoundTripper
}

func newTransport(accessKeyID, secretAccessKey string) http.RoundTripper {
	t := http.DefaultTransport
	if accessKeyID != "" && secretAccessKey != "" {
		return &AWSTransport{
			Credentials: awsauth.Credentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretAccessKey,
			},
			transport: t,
		}
	}
	return t
}

// RoundTrip implementation
func (a AWSTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	awsauth.Sign4(req, a.Credentials)
	return a.transport.RoundTrip(req)
}
