package integration

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	mgo "gopkg.in/mgo.v2"

	"github.com/compose/transporter/pkg/log"
)

var (
	mongodbSinkSession *mgo.Session
	cleanup            = flag.Bool("cleanup", false, "used to determien whether or not to run cleanup function")
)

func setup() {
	log.Infoln("setting up tests")
	uri := fmt.Sprintf("mongodb://%s:%s@%s/enron",
		os.Getenv("MONGODB_ENRON_SINK_USER"),
		os.Getenv("MONGODB_ENRON_SINK_PASSWORD"),
		os.Getenv("MONGODB_ENRON_SINK_URI"))
	dialInfo, _ := mgo.ParseURL(uri)
	dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
		return tls.Dial("tcp", addr.String(), &tls.Config{InsecureSkipVerify: true})
	}
	dialInfo.Timeout = 5 * time.Second
	mgoSession, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		log.Errorf("DialWithInfo failed, %s", err)
	}
	mongodbSinkSession = mgoSession
}

func cleanupData() {
	log.Infoln("cleaning up data")
	mongodbSinkSession.DB("enron").C("emails").DropCollection()
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
	mongodbSinkSession.Close()
	log.Infoln("tests shutdown complete")
}
