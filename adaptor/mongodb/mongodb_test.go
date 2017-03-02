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

var initTests = []map[string]interface{}{
	{"uri": DefaultURI, "namespace": "test.test"},
	{"uri": DefaultURI, "namespace": "test.test", "tail": true},
	{"uri": DefaultURI, "namespace": "test.test", "bulk": true},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := adaptor.GetAdaptor("mongodb", it)
		if err != nil {
			t.Fatalf("unexpected GetV2() error, %s", err)
		}
		if _, err := a.Client(); err != nil {
			t.Errorf("unexpected Client() error, %s", err)
		}
		if _, err := a.Reader(); err != nil {
			t.Errorf("unexpected Reader() error, %s", err)
		}
		done := make(chan struct{})
		var wg sync.WaitGroup
		if _, err := a.Writer(done, &wg); err != nil {
			t.Errorf("unexpected Writer() error, %s", err)
		}
		close(done)
		wg.Wait()
	}
}
