package etcd

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/compose/transporter/pkg/log"

	eclient "github.com/coreos/etcd/client"
)

var (
	defaultTestURI    = DefaultEndpoints
	defaultTestClient = &Client{
		cfg: eclient.Config{
			Endpoints:               DefaultEndpoints,
			HeaderTimeoutPerRequest: DefaultRequestTimeout,
			Transport:               eclient.DefaultTransport,
		},
	}
	defaultSession *Session

	defaultKeysAPI eclient.KeysAPI

	dirsToTest = [][]TestData{mockData}
)

type TestData struct {
	rootKey string
	key     string
	value   string
}

var mockData = []TestData{
	{"", "message", "Welcome"},
	{"", "messages/english", "Hello World"},
	{"", "messages/spanish", "Hola world"},
	{"", "messages/languages/go", "is awesome"},
	{"", "messages/languages/ruby", "is pretty good"},
	{"", "messages/languages/java", "is lame"},
	{"subkey", "message", "Welcome"},
	{"subkey", "messages/english", "Hello World"},
	{"subkey", "messages/spanish", "Hola world"},
	{"subkey", "messages/languages/go", "is awesome"},
	{"subkey", "messages/languages/ruby", "is pretty good"},
	{"subkey", "messages/languages/java", "is lame"},
}

func dataBySubKey(subkey string) []TestData {
	subkeyData := make([]TestData, 0)
	for _, d := range mockData {
		if d.rootKey == subkey {
			subkeyData = append(subkeyData, d)
		}
	}
	return subkeyData
}

func setup() {
	log.Infoln("setting up tests")

	s, err := defaultTestClient.Connect()
	if err != nil {
		log.Errorf("unable to initialize connection to etcd, %s", err)
		os.Exit(1)
	}
	defaultSession = s.(*Session)

	defaultKeysAPI = eclient.NewKeysAPI(defaultSession.Client)

	clearTestData()

	setupTestData()
}

func clearTestData() {
	resp, err := defaultKeysAPI.Get(context.Background(), "/", nil)
	if err != nil {
		log.Errorf("unable to list root director in etcd, %s", err)
		os.Exit(1)
	}
	for _, n := range resp.Node.Nodes {
		_, err := defaultKeysAPI.Delete(context.Background(), n.Key, &eclient.DeleteOptions{Recursive: true})
		if err != nil {
			log.Errorf("unable to remove key (%s) in etcd, %s", n.Key, err)
			os.Exit(1)
		}
	}
}

func setupTestData() {
	for _, dirs := range dirsToTest {
		for _, mockData := range dirs {
			_, err := defaultKeysAPI.Create(context.Background(), fmt.Sprintf("%s/%s", mockData.rootKey, mockData.key), mockData.value)
			if err != nil {
				log.Errorf("unable to insert mock data, %s", err)
				os.Exit(1)
			}
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
	clearTestData()
	defaultSession.Close()
	log.Infoln("tests shutdown complete")
}
