package error

import "fmt"

// InvalidURIError wraps the underlying error when the provided URI is not parsable by mgo.
type InvalidURIError struct {
	URI       string
	ErrDetail string
}

func (e InvalidURIError) Error() string {
	return fmt.Sprintf("Invalid URI (%s), %s", e.URI, e.ErrDetail)
}

// InvalidTimeoutError wraps the underlying error when the provided is not parsable time.ParseDuration.
type InvalidTimeoutError struct {
	Timeout string
}

func (e InvalidTimeoutError) Error() string {
	return fmt.Sprintf("Invalid Timeout, %s", e.Timeout)
}

// ConnectError wraps the underlying error when a failure occurs dialing the database.
type ConnectError struct {
	Reason string
}

func (e ConnectError) Error() string {
	return fmt.Sprintf("connection error, %s", e.Reason)
}
