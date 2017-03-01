// +build integration

package integration_test

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/compose/transporter/log"
)

var (
	cleanup = flag.Bool("cleanup", false, "used to determien whether or not to run cleanup function")
)

func setup() {
	log.Infoln("setting up tests")
}

func cleanupData() {
	log.Infoln("cleaning up data")
	req, _ := http.NewRequest(
		http.MethodDelete,
		fmt.Sprintf("https://%s/enron", strings.Split(os.Getenv("ES_ENRON_SINK_URI"), ",")[0]),
		nil)
	req.SetBasicAuth(os.Getenv("ES_ENRON_SINK_USER"), os.Getenv("ES_ENRON_SINK_PASSWORD"))
	if _, err := http.DefaultClient.Do(req); err != nil {
		log.Errorf("delete request errored, %s", err)
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
