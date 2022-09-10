package client

import (
	"errors"
	"fmt"
)

// InvalidURIError wraps the underlying error when the provided URI is not parsable by mgo.
type InvalidURIError struct {
	URI string
	Err string
}

func (e InvalidURIError) Error() string {
	return fmt.Sprintf("Invalid URI (%s), %s", e.URI, e.Err)
}

// InvalidTimeoutError wraps the underlying error when the provided is not parsable time.ParseDuration.
type InvalidTimeoutError struct {
	Timeout string
}

func (e InvalidTimeoutError) Error() string {
	return fmt.Sprintf("Invalid Timeout, %s", e.Timeout)
}

type InvalidCertificateError struct {
	Err string
}

func (e InvalidCertificateError) Error() string {
	return fmt.Sprintf("Invalid Certificate, %s", e.Err)
}

// ErrInvalidCert represents the error returned when a specified certificate was not valid
var ErrInvalidCert = errors.New("invalid cert error")

// ConnectError wraps the underlying error when a failure occurs dialing the database.
type ConnectError struct {
	Reason string
}

func (e ConnectError) Error() string {
	return fmt.Sprintf("connection error, %s", e.Reason)
}

// VersionError represents any failure in attempting to obtain the version from the provided uri.
type VersionError struct {
	URI string
	V   string
	Err string
}

func (e VersionError) Error() string {
	if e.V == "" {
		return fmt.Sprintf("unable to determine version from %s, %s", e.URI, e.Err)
	}
	return fmt.Sprintf("%s running %s, %s", e.URI, e.V, e.Err)
}
