package pipeline

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/compose/transporter/adaptor"
	_ "github.com/compose/transporter/adaptor/all"
	"github.com/compose/transporter/events"
)

// set up some local files
func setupFiles(in, out string) {
	// setup files
	os.Remove(out)
	os.Remove(in)

	fh, _ := os.Create(out)
	defer fh.Close()
	fh.WriteString("{\"_id\":\"546656989330a846dc7ce327\",\"test\":\"hello world\"}\n")
	fh.WriteString("{\"_id\":\"546656989330a846dc7ce328\",\"test\":\"hello world 2\"}\n")

}

func TestFileToFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping FileToFile in short mode")

	}
	var (
		tempDir = os.TempDir()
		inFile  = filepath.Join(tempDir, "in")
		outFile = filepath.Join(tempDir, "out")
	)

	setupFiles(inFile, outFile)

	numgorosBefore := runtime.NumGoroutine()
	// create the source node and attach our sink
	f, err := adaptor.GetAdaptor("file", adaptor.Config{"uri": "file://" + outFile})
	if err != nil {
		t.Fatalf("can't create GetAdaptor, got %s", err)
	}
	outNode, err := NewNode("localfileout", "file", "blah./.*/", f, nil)
	if err != nil {
		t.Fatalf("can't create newnode, got %s", err)
	}
	f, err = adaptor.GetAdaptor("file", adaptor.Config{"uri": "file://" + inFile})
	if err != nil {
		t.Fatalf("can't create GetAdaptor, got %s", err)
	}
	_, err = NewNode("localfilein", "file", "blah./.*/", f, outNode)
	if err != nil {
		t.Fatalf("can't create newnode, got %s", err)
	}

	// create the pipeline
	p, err := NewPipeline("test", outNode, events.LogEmitter(), 60*time.Second, nil, 10*time.Second)
	if err != nil {
		t.Errorf("can't create pipeline, got %s", err.Error())
		t.FailNow()
	}

	// run it
	err = p.Run()
	if err != nil {
		t.Errorf("error running pipeline, got %s", err.Error())
		t.FailNow()
	}

	p.Stop()
	time.Sleep(1 * time.Second)
	numgorosAfter := runtime.NumGoroutine()
	if numgorosBefore < numgorosAfter {
		trace := make([]byte, 10240)
		runtime.Stack(trace, true)
		t.Errorf("leaky goroutines detected, started with %d, ended with %d\n%s", numgorosBefore, numgorosAfter, trace)
	}

	// compare the files
	sourceFile, err := os.Open(outFile)
	if err != nil {
		t.Errorf("error opening source file %s, got %v", outFile, err)
		t.FailNow()
	}
	sourceSize, err := sourceFile.Stat()
	if err != nil {
		t.Errorf("error statting source file %s, got %v", outFile, err)
		t.FailNow()
	}
	defer sourceFile.Close()
	sinkFile, err := os.Open(inFile)
	if err != nil {
		t.Errorf("error opening sink file %s, got %v", inFile, err)
		t.FailNow()
	}
	sinkSize, err := sinkFile.Stat()
	if err != nil {
		t.Errorf("error statting sink file %s, got %v", inFile, err)
		t.FailNow()
	}
	defer sinkFile.Close()

	if sourceSize.Size() == 0 || sourceSize.Size() != sinkSize.Size() {
		t.Errorf("Incorrect file size\nexp %d\ngot %d", sourceSize.Size(), sinkSize.Size())
	}
}
