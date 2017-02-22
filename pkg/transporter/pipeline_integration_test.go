package transporter

import (
	"os"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/adaptor"
	_ "github.com/compose/transporter/pkg/adaptor/all"
)

// set up some local files
func setupFiles(in, out string) {
	// setup files
	os.Remove(out)
	os.Remove(in)

	fh, _ := os.Create(out)
	defer fh.Close()
	fh.WriteString("{\"_id\":\"546656989330a846dc7ce327\",\"test\":\"hello world\"}\n")
}

func TestFileToFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping FileToFile in short mode")
	}
	var (
		inFile  = "/tmp/crapIn"
		outFile = "/tmp/crapOut"
	)

	setupFiles(inFile, outFile)

	// create the source node and attach our sink
	outNode := NewNode("localfileout", "file", adaptor.Config{"uri": "file://" + outFile}).
		Add(NewNode("localfilein", "file", adaptor.Config{"uri": "file://" + inFile}))

	// create the pipeline
	p, err := NewDefaultPipeline(outNode, "", "", "", "test", 100*time.Millisecond)
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

	// compare the files
	sourceFile, _ := os.Open(outFile)
	sourceSize, _ := sourceFile.Stat()
	defer sourceFile.Close()
	sinkFile, _ := os.Open(inFile)
	sinkSize, _ := sinkFile.Stat()
	defer sinkFile.Close()

	if sourceSize.Size() == 0 || sourceSize.Size() != sinkSize.Size() {
		t.Errorf("Incorrect file size\nexp %d\ngot %d", sourceSize.Size(), sinkSize.Size())
	}
}
