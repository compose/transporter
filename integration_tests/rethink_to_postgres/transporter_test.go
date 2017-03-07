// +build integration

package integration_test

import (
	"testing"
)

func TestRethinkToPostgresDocCount(t *testing.T) {
	var count int
	err := postgresSourceSession.QueryRow("SELECT COUNT(id) FROM emails;").Scan(&count)
	if err != nil {
		t.Errorf("unable to count table, %s", err)
	}
	if count != 5477 {
		t.Errorf("bad emailCount, expected 5477, got %d", count)
	}
}
