package rabbitmq

import (
	"testing"

	"github.com/compose/transporter/adaptor"
)

func TestDescription(t *testing.T) {
	r := rabbitMQ{}
	if r.Description() != description {
		t.Errorf("unexpected Description, expected %s, got %s\n", description, r.Description())
	}
}

func TestSampleConfig(t *testing.T) {
	r := rabbitMQ{}
	if r.SampleConfig() != sampleConfig {
		t.Errorf("unexpected SampleConfig, expected %s, got %s\n", sampleConfig, r.SampleConfig())
	}
}

var initTests = []map[string]interface{}{
	{},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := adaptor.GetAdaptor("rabbitmq", it)
		if err != nil {
			t.Fatalf("unexpected GetAdaptor() error, %s", err)
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
