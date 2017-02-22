// +build integration

package integration_test

import "testing"

func TestMongoToMongoDocCount(t *testing.T) {
	emailCount, err := mongodbSinkSession.DB("enron").C("emails").Count()
	if err != nil {
		t.Fatalf("unable to count emails, %s", err)
	}

	if emailCount != 501514 {
		t.Errorf("bad emailCount, expected 501514, got %d", emailCount)
	}
}
