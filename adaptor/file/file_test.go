package file

import (
	"testing"

	"github.com/compose/transporter/adaptor"
)

func TestDescription(t *testing.T) {
	f := File{}
	if f.Description() != description {
		t.Errorf("unexpected Description, expected %s, got %s\n", description, f.Description())
	}
}

func TestSampleConfig(t *testing.T) {
	f := File{}
	if f.SampleConfig() != sampleConfig {
		t.Errorf("unexpected SampleConfig, expected %s, got %s\n", sampleConfig, f.SampleConfig())
	}
}

var initTests = []map[string]interface{}{
	{"uri": DefaultURI},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := adaptor.GetAdaptor("file", it)
		if err != nil {
			t.Fatalf("unexpected GetV2() error, %s", err)
		}
		if _, err := a.Client(); err != nil {
			t.Errorf("unexpected Client() error, %s", err)
		}
		if _, err := a.Reader(); err != nil {
			t.Errorf("unexpected Reader() error, %s", err)
		}
		if _, err := a.Writer(nil, nil); err != nil {
			t.Errorf("unexpected Writer() error, %s", err)
		}
	}
}
