package log

import (
	"bytes"
	"regexp"
	"testing"

	"github.com/Sirupsen/logrus"
)

func TestFileLineLogging(t *testing.T) {
	var buf bytes.Buffer
	origLogger.Out = &buf
	origLogger.Formatter = &logrus.TextFormatter{
		DisableColors: true,
	}

	// The default logging level should be "info".
	Debugln("This debug-level line should not show up in the output.")
	Infof("This %s-level line should show up in the output.", "info")

	re := `^time=".*" level=info msg="This info-level line should show up in the output." \n$`
	if !regexp.MustCompile(re).Match(buf.Bytes()) {
		t.Fatalf("%q did not match expected regex %q", buf.String(), re)
	}
}
