package mysql
import (
	"testing"

	"github.com/compose/transporter/adaptor"
)

func TestDescription(t *testing.T) {
	p := &mysql{}
	if p.Description() != description {
		t.Errorf("unexpected Description, expected %s, got %s\n", description, p.Description())
	}
}

func TestSampleConfig(t *testing.T) {
	p := &mysql{}
	if p.SampleConfig() != sampleConfig {
		t.Errorf("unexpected SampleConfig, expected %s, got %s\n", sampleConfig, p.SampleConfig())
	}
}

var initTests = []map[string]interface{}{
	{"uri": DefaultURI},
	{"uri": DefaultURI, "tail": true},
}

func TestInit(t *testing.T) {
	for _, it := range initTests {
		a, err := adaptor.GetAdaptor("mysql", it)
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
