// +build integration

package integration_test

import (
	"testing"

	r "gopkg.in/gorethink/gorethink.v3"
)

func TestMongoToRethinkDocCount(t *testing.T) {
	cursor, err := r.Table("emails").Count().Run(rethinkSinkSession)
	if err != nil {
		t.Fatalf("unable to count emails, %s", err)
	}
	var emailCount int
	cursor.One(&emailCount)
	cursor.Close()

	if emailCount != 501514 {
		t.Errorf("bad emailCount, expected 501514, got %d", emailCount)
	}
}
