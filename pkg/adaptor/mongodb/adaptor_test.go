package mongodb

import (
	"os"
	"testing"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/pkg/log"
)

var (
	defaultTestURI    = DefaultURI
	defaultTestClient = &Client{
		uri:            DefaultURI,
		sessionTimeout: DefaultSessionTimeout,
		safety:         DefaultSafety,
	}
	defaultSession *Session

	dbsToTest = []*TestData{startTestData, listenTestData, bulkTestData, readerTestData, writerTestData, tailTestData}
)

type TestData struct {
	DB          string
	C           string
	InsertCount int
}

func setup() {
	log.Infoln("setting up tests")

	s, err := defaultTestClient.Connect()
	if err != nil {
		log.Errorf("unable to initialize connection to mongodb, %s", err)
		os.Exit(1)
	}
	defaultSession = s.(*Session)
	for _, testData := range dbsToTest {
		setupData(testData)
	}
}

func setupData(data *TestData) {
	if err := defaultSession.mgoSession.DB(data.DB).DropDatabase(); err != nil {
		log.Errorf("failed to drop database (%s), may affect tests!, %s", data.DB, err)
	}
	for i := 0; i < data.InsertCount; i++ {
		defaultSession.mgoSession.DB(data.DB).C(data.C).Insert(bson.M{"_id": i, "i": i})
	}
}

func TestMain(m *testing.M) {
	setup()
	mgo.SetLogger(nil)
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func shutdown() {
	log.Infoln("shutting down tests")
	defaultSession.Close()
	log.Infoln("tests shutdown complete")
}
