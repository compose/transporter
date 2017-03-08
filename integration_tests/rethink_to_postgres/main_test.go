// +build integration

package integration_test

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/compose/transporter/log"

	_ "github.com/lib/pq" // import pq driver
)

const (
	emailsSchema = `id varchar(24),
  body TEXT,
  filename varchar(255),
  headers jsonb,
  mailbox varchar(255),
  subFolder varchar(255),

  PRIMARY KEY (id)`
)

var (
	postgresSourceSession *sql.DB
	cleanup               = flag.Bool("cleanup", false, "used to determine whether or not to run cleanup function")
)

func setup() {
	log.Infoln("setting up tests")
	u := fmt.Sprintf("postgres://%s:%s@%s",
		os.Getenv("POSTGRES_ENRON_SINK_USER"),
		os.Getenv("POSTGRES_ENRON_SINK_PASSWORD"),
		os.Getenv("POSTGRES_ENRON_SINK_URI"))
	postgresSourceSession, _ = sql.Open("postgres", u)
}

func cleanupData() {
	log.Infoln("cleaning up data")

	if _, err := postgresSourceSession.Exec("DROP TABLE IF EXISTS emails;"); err != nil {
		log.Errorf("unable to drop table, could affect tests, %s", err)
	}

	_, err := postgresSourceSession.Exec(fmt.Sprintf("CREATE TABLE emails ( %s );", emailsSchema))
	if err != nil {
		log.Errorf("unable to create table, could affect tests, %s", err)
	}
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
	log.Infoln("tests shutdown complete")
}
