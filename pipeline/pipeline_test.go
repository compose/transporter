package pipeline

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/compose/transporter/adaptor"
	_ "github.com/compose/transporter/adaptor/file"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/commitlog"
	"github.com/compose/transporter/events"
	"github.com/compose/transporter/offset"
)

// a noop node adaptor to help test
type Testadaptor struct {
	value string
}

func init() {
	adaptor.Add(
		"source",
		func() adaptor.Adaptor {
			return &adaptor.Mock{}
		},
	)
}

func (s *Testadaptor) Description() string {
	return "description"
}

func (s *Testadaptor) SampleConfig() string {
	return ""
}

func TestPipelineString(t *testing.T) {
	data := []struct {
		in  func() *Node
		out string
	}{
		{
			func() *Node {
				n, _ := NewNodeWithOptions("source1", "source", defaultNsString)
				NewNodeWithOptions("localfile", "file", defaultNsString, WithParent(n))
				return n
			},
			` - Source:         source1                                  source          .*                            
  - Sink:          localfile                                file            .*                            `,
		},
	}

	mockTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mockTS.Close()

	for _, v := range data {
		node := v.in()
		p, err := NewDefaultPipeline(node, mockTS.URL, "test", "test", "test", 100*time.Millisecond)
		if err != nil {
			t.Errorf("can't create pipeline, got %s", err.Error())
			t.FailNow()
		}
		actual := p.String()
		if actual != v.out {
			t.Errorf("\nexpected:\n%v\ngot:\n%v\n", v.out, actual)
		}

		close(p.done)
		p.emitMetrics()
		p.emitter.Stop()
		close(p.source.pipe.Err)
	}
}

var (
	runTests = []struct {
		sourceNode func() *Node
		runErr     error
	}{
		{
			func() *Node {
				a := &adaptor.Mock{}
				n, _ := NewNodeWithOptions(
					"starter", "stopWriter", defaultNsString,
					WithClient(a),
					WithReader(a),
					WithCommitLog([]commitlog.OptionFunc{
						commitlog.WithPath("testdata/pipeline_run"),
					}...),
				)
				NewNodeWithOptions(
					"stopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithOffsetManager(&offset.MockManager{MemoryMap: map[string]uint64{}}),
				)
				return n
			},
			nil,
		},
		{
			func() *Node {
				a := &adaptor.Mock{}
				n, _ := NewNodeWithOptions(
					"starter", "stopWriter", defaultNsString,
					WithClient(&adaptor.MockClientErr{}),
					WithReader(a),
					WithCommitLog([]commitlog.OptionFunc{
						commitlog.WithPath("testdata/pipeline_run"),
					}...),
				)
				NewNodeWithOptions(
					"stopper", "stopWriter", defaultNsString,
					WithClient(a),
					WithWriter(a),
					WithParent(n),
					WithOffsetManager(&offset.MockManager{MemoryMap: map[string]uint64{}}),
				)
				return n
			},
			client.ErrMockConnect,
		},
		// uncomment this once the error handling mess is sorted out
		// {
		// 	func() *Node {
		// 		a := &adaptor.Mock{}
		// 		n, _ := NewNodeWithOptions(
		// 			"starter", "stopWriter", defaultNsString,
		// 			WithClient(a),
		// 			WithReader(a),
		// 			WithCommitLog("testdata/restart_from_end", 1024),
		// 		)
		// 		NewNodeWithOptions(
		// 			"stopperWriteErr", "stopWriter", defaultNsString,
		// 			WithClient(a),
		// 			WithWriter(&adaptor.MockWriterErr{}),
		// 			WithParent(n),
		// 			WithOffsetManager(&offset.MockManager{MemoryMap: map[string]uint64{}}),
		// 		)
		// 		return n
		// 	},
		// 	client.ErrMockWrite,
		// },
	}
)

func TestRun(t *testing.T) {
	for _, rt := range runTests {
		source := rt.sourceNode()
		p, err := NewPipeline("test", source, events.LogEmitter(), 1*time.Second)
		if err != nil {
			t.Fatalf("unexpected NewPipeline error, %s", err)
		}
		if err := p.Run(); err != rt.runErr {
			t.Errorf("wrong Run error, expected %s, got %s", rt.runErr, err)
		}
		p.Stop()
	}
}
