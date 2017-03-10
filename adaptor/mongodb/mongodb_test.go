package mongodb

import (
	"sync"
	"testing"

	"github.com/compose/transporter/adaptor"
)

func TestDescription(t *testing.T) {
	m := MongoDB{}
	if m.Description() != description {
		t.Errorf("unexpected Description, expected %s, got %s\n", description, m.Description())
	}
}

func TestSampleConfig(t *testing.T) {
	m := MongoDB{}
	if m.SampleConfig() != sampleConfig {
		t.Errorf("unexpected SampleConfig, expected %s, got %s\n", sampleConfig, m.SampleConfig())
	}
}

var initTests = []struct {
	cfg       map[string]interface{}
	clientErr error
	readerErr error
	writerErr error
}{
	{
		map[string]interface{}{"uri": DefaultURI, "namespace": "test.test"}, nil, nil, nil,
	},
	{
		map[string]interface{}{"uri": DefaultURI, "namespace": "test.test", "tail": true}, nil, nil, nil,
	},
	{
		map[string]interface{}{"uri": DefaultURI, "namespace": "test.test", "bulk": true}, nil, nil, nil,
	},
	{
		map[string]interface{}{"uri": DefaultURI, "namespace": "test.test", "collection_filters": `{"foo":{"i":{"$gt":10}}}`}, nil, nil, nil,
	},
	{
		map[string]interface{}{"uri": DefaultURI, "namespace": "test.test", "collection_filters": `{"foo":{"i":{"$gt":10}}`}, nil, ErrCollectionFilter, nil,
	},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := adaptor.GetAdaptor("mongodb", it.cfg)
		if err != nil {
			t.Fatalf("unexpected GetV2() error, %s", err)
		}
		if _, err := a.Client(); err != it.clientErr {
			t.Errorf("unexpected Client() error, %s", err)
		}
		if _, err := a.Reader(); err != it.readerErr {
			t.Errorf("unexpected Reader() error, %s", err)
		}
		done := make(chan struct{})
		var wg sync.WaitGroup
		if _, err := a.Writer(done, &wg); err != it.writerErr {
			t.Errorf("unexpected Writer() error, %s", err)
		}
		close(done)
		wg.Wait()
	}
}
