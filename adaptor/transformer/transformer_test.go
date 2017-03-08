package transformer

import (
	"testing"

	"github.com/compose/transporter/adaptor"
)

func TestDescription(t *testing.T) {
	transformer := Transformer{}
	if transformer.Description() != description {
		t.Errorf("unexpected Description, expected %s, got %s\n", description, transformer.Description())
	}
}

func TestSampleConfig(t *testing.T) {
	transformer := Transformer{}
	if transformer.SampleConfig() != sampleConfig {
		t.Errorf("unexpected SampleConfig, expected %s, got %s\n", sampleConfig, transformer.SampleConfig())
	}
}

var initTests = []map[string]interface{}{
	{"filename": "testdata/transformer.js"},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := adaptor.GetAdaptor("transformer", it)
		if err != nil {
			t.Fatalf("unexpected GetV2() error, %s", err)
		}
		if _, err := a.Client(); err != nil {
			t.Errorf("unexpected Client() error, %s", err)
		}
		rerr := adaptor.ErrFuncNotSupported{Name: "transformer", Func: "Reader()"}
		if _, err := a.Reader(); err != rerr {
			t.Errorf("wrong Reader() error, expected %s, got %s", rerr, err)
		}
		if _, err := a.Writer(nil, nil); err != nil {
			t.Errorf("unexpected Writer() error, %s", err)
		}
	}
}
