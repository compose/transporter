package gou

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSetupLogToFile(t *testing.T) {
	tmpf, err := ioutil.TempFile("", "goutest")
	if err != nil {
		t.Fatalf("error creating log file: %v\n", err)
	}
	defer os.Remove(tmpf.Name())

	SetupLoggingFile(tmpf, "debug")
	logStr := "hihi"
	Infof(logStr)

	// Flush file buffer to disk
	err = tmpf.Sync()
	if err != nil {
		t.Errorf("error syncing tmpf: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Read tmp file and confirm log message was written
	bytes, err := ioutil.ReadFile(tmpf.Name())
	if err != nil {
		t.Errorf("error reading temp file[%s]: %v\n", tmpf.Name(), err)
	}

	logFileBytes := string(bytes)
	if !strings.Contains(logFileBytes, logStr) {
		t.Logf("logfile:\n%s", logFileBytes)
		t.Errorf("%s not found in logfile %s\n", logStr, tmpf.Name())
	}
}

func TestLogrusLogger(t *testing.T) {
	SetupLogrus("debug")

	Debug("Debug")
	Infof("Info")
	Warn("Warn")
	Error("Error")

	rus = nil
}
