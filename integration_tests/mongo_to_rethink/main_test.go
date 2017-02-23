// +build integration

package integration_test

import (
	"crypto/tls"
	"flag"
	"os"
	"testing"

	r "gopkg.in/gorethink/gorethink.v3"

	"github.com/compose/transporter/pkg/log"
)

var (
	rethinkSinkSession *r.Session
	cleanup            = flag.Bool("cleanup", false, "used to determine whether or not to run cleanup function")
)

func setup() {
	log.Infoln("setting up tests")
	s, err := r.Connect(r.ConnectOpts{
		Address:   os.Getenv("RETHINKDB_ENRON_SINK_URI"),
		Username:  os.Getenv("RETHINKDB_ENRON_SINK_USER"),
		Password:  os.Getenv("RETHINKDB_ENRON_SINK_PASSWORD"),
		Database:  "enron",
		TLSConfig: &tls.Config{InsecureSkipVerify: true},
	})
	if err != nil {
		log.Errorf("Connect failed, %s", err)
	}
	rethinkSinkSession = s
}

func cleanupData() {
	log.Infoln("cleaning up data")
	resp, err := r.TableDrop("emails").RunWrite(rethinkSinkSession)
	if err != nil {
		log.Errorf("unable to drop table, could affect tests, %s", err)
	}
	log.Infof("TableDrop response, %+v", resp)
	resp, err = r.TableCreate("emails", r.TableCreateOpts{Shards: 3, Replicas: 2}).RunWrite(rethinkSinkSession)
	if err != nil {
		log.Errorf("unable to create table, will definitely affect tests, %s", err)
	}
	log.Infof("TableCreate response, %+v", resp)
}

func TestMain(m *testing.M) {
	flag.Parse()
	setup()
	if *cleanup {
		cleanupData()
		shutdown()
		os.Exit(0)
		return
	}
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func shutdown() {
	log.Infoln("shutting down tests")
	rethinkSinkSession.Close()
	log.Infoln("tests shutdown complete")
}
