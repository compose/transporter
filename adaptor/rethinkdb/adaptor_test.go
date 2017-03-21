package rethinkdb

import (
	"os"
	"testing"

	"github.com/compose/transporter/log"

	r "gopkg.in/gorethink/gorethink.v3"
)

var (
	defaultTestClient = &Client{
		uri:            DefaultURI,
		sessionTimeout: DefaultTimeout,
	}
	defaultSession *Session

	dbsToTest = []*TestData{readerTestData, tailTestData, writerTestData}
)

type TestData struct {
	DB          string
	T           string
	InsertCount int
}

func setup() {
	log.Infoln("setting up tests")

	s, err := defaultTestClient.Connect()
	if err != nil {
		log.Errorf("unable to initialize connection to rethinkdb, %s", err)
		os.Exit(1)
	}
	defaultSession = s.(*Session)
	for _, testData := range dbsToTest {
		setupData(testData)
	}
}

func setupData(data *TestData) {
	if _, err := r.DBDrop(data.DB).RunWrite(defaultSession.session); err != nil {
		log.Errorf("failed to drop database (%s), may affect tests!, %s", data.DB, err)
	}

	if _, err := r.DBCreate(data.DB).RunWrite(defaultSession.session); err != nil {
		log.Errorf("failed to create database (%s), may affect tests!, %s", data.DB, err)
	}

	if _, err := r.DB(data.DB).TableCreate(data.T).RunWrite(defaultSession.session); err != nil {
		log.Errorf("failed to create table (%s) in %s, may affect tests!, %s", data.T, data.DB, err)
	}

	for i := 0; i < data.InsertCount; i++ {
		_, err := r.DB(data.DB).Table(data.T).Insert(map[string]interface{}{"id": i, "i": i}).RunWrite(defaultSession.session)
		if err != nil {
			log.Errorf("failed to write mock data, may affect tests!, %s", err)
		}
	}
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func shutdown() {
	log.Infoln("shutting down tests")
	defaultClient.Close()
	log.Infoln("tests shutdown complete")
}
