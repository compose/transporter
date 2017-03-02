package pipeline

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/compose/transporter/adaptor"
	_ "github.com/compose/transporter/adaptor/all"
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

	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	}))
	defer ts.Close()
	ts.Start()

	var (
		tempDir = os.TempDir()
		inFile  = filepath.Join(tempDir, "in")
		outFile = filepath.Join(tempDir, "out")
	)

	setupFiles(inFile, outFile)

	// create the source node and attach our sink
	outNode := NewNode("localfileout", "file", adaptor.Config{"uri": "file://" + outFile}).
		Add(NewNode("localfilein", "file", adaptor.Config{"uri": "file://" + inFile}))

	// create the pipeline
	p, err := NewDefaultPipeline(outNode, ts.URL, "", "", "test", 100*time.Millisecond)
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
