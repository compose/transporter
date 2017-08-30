package mongodb

import (
	"reflect"
	"sync"
	"testing"

	"github.com/compose/transporter/adaptor"
)

func TestDescription(t *testing.T) {
	m := mongoDB{}
	if m.Description() != description {
		t.Errorf("unexpected Description, expected %s, got %s\n", description, m.Description())
	}
}

func TestSampleConfig(t *testing.T) {
	m := mongoDB{}
	if m.SampleConfig() != sampleConfig {
		t.Errorf("unexpected SampleConfig, expected %s, got %s\n", sampleConfig, m.SampleConfig())
	}
}

var initTests = []struct {
	name      string
	cfg       map[string]interface{}
	mongodb   *mongoDB
	clientErr error
	readerErr error
	writerErr error
}{
	{
		"base",
		map[string]interface{}{"uri": DefaultURI},
		&mongoDB{BaseConfig: adaptor.BaseConfig{URI: DefaultURI}},
		nil, nil, nil,
	},
	{
		"with timeout",
		map[string]interface{}{"uri": DefaultURI, "timeout": "60s"},
		&mongoDB{BaseConfig: adaptor.BaseConfig{URI: DefaultURI, Timeout: "60s"}},
		nil, nil, nil,
	},
	{
		"with tail",
		map[string]interface{}{"uri": DefaultURI, "tail": true},
		&mongoDB{BaseConfig: adaptor.BaseConfig{URI: DefaultURI}, Tail: true},
		nil, nil, nil,
	},
	{
		"with bulk",
		map[string]interface{}{"uri": DefaultURI, "bulk": true},
		&mongoDB{BaseConfig: adaptor.BaseConfig{URI: DefaultURI}, Bulk: true},
		nil, nil, nil,
	},
	{
		"with collection filters",
		map[string]interface{}{"uri": DefaultURI, "collection_filters": `{"foo":{"i":{"$gt":10}}}`},
		&mongoDB{BaseConfig: adaptor.BaseConfig{URI: DefaultURI}, CollectionFilters: `{"foo":{"i":{"$gt":10}}}`},
		nil, nil, nil,
	},
	{
		"bad collection filter",
		map[string]interface{}{"uri": DefaultURI, "collection_filters": `{"foo":{"i":{"$gt":10}}`},
		&mongoDB{BaseConfig: adaptor.BaseConfig{URI: DefaultURI}, CollectionFilters: `{"foo":{"i":{"$gt":10}}`},
		nil, ErrCollectionFilter, nil,
	},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := adaptor.GetAdaptor("mongodb", it.cfg)
		if err != nil {
			t.Fatalf("[%s] unexpected GetV2() error, %s", it.name, err)
		}
		if !reflect.DeepEqual(a, it.mongodb) {
			t.Errorf("[%s] wrong struct, expected %+v, got %+v", it.name, it.mongodb, a)
		}
		if _, err := a.Client(); err != it.clientErr {
			t.Errorf("[%s] unexpected Client() error, %s", it.name, err)
		}
		if _, err := a.Reader(); err != it.readerErr {
			t.Errorf("[%s] unexpected Reader() error, %s", it.name, err)
		}
		done := make(chan struct{})
		var wg sync.WaitGroup
		if _, err := a.Writer(done, &wg); err != it.writerErr {
			t.Errorf("[%s] unexpected Writer() error, %s", it.name, err)
		}
		close(done)
		wg.Wait()
	}
}
