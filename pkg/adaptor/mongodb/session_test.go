package mongodb

import (
	"testing"

	mgo "gopkg.in/mgo.v2"
)

func TestClose(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Close in short mode")
	}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic but should have")
		}
	}()

	mgoSession, err := mgo.Dial(DefaultURI)
	if err != nil {
		t.Fatalf("unable to dial mongodb, %s\n", err)
	}
	mgoSession.DB("transporter_test").DropDatabase()
	s, err := &Session{mgoSession}, nil
	if err != nil {
		t.Fatalf("unable to dial mongodb, %s\n", err)
	}
	s.Close()
	mgoSession.Ping()
}
