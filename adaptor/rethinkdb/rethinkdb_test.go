package rethinkdb

import (
	"sync"
	"testing"

	"github.com/compose/transporter/adaptor"
)

func TestDescription(t *testing.T) {
	r := RethinkDB{}
	if r.Description() != description {
		t.Errorf("unexpected Description, expected %s, got %s\n", description, r.Description())
	}
}

func TestSampleConfig(t *testing.T) {
	r := RethinkDB{}
	if r.SampleConfig() != sampleConfig {
		t.Errorf("unexpected SampleConfig, expected %s, got %s\n", sampleConfig, r.SampleConfig())
	}
}

var initTests = []map[string]interface{}{
	{"uri": DefaultURI},
	{"uri": DefaultURI, "tail": true},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := adaptor.GetAdaptor("rethinkdb", it)
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
