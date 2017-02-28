package file

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/compose/transporter/log"
)

var (
	testTmpDir = func(prefix string) string {
		t, _ := ioutil.TempDir("", prefix)
		return t
	}
)

func setup() {
	log.Infoln("setting up tests")
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func shutdown() {
	log.Infoln("shutting down tests")
	log.Infoln("tests shutdown complete")
}
