package log_test

import (
	"bytes"
	"flag"
	"regexp"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/compose/transporter/log"
)

var (
	loggerTests = []struct {
		logLines func()
		logLevel logrus.Level
		re       *regexp.Regexp
	}{
		{
			func() {
				log.Base().Debugln("This debug-level line should not show up in the output.")
				log.Base().Infof("This %s-level line should show up in the output.", "info")
			},
			logrus.InfoLevel,
			regexp.MustCompile(`^time=".*" level=info msg="This info-level line should show up in the output." \n$`),
		},
		{
			func() {
				log.Debugf("This %s-level line should show up in the output.", "debug")
			},
			logrus.DebugLevel,
			regexp.MustCompile(`^time=".*" level=debug msg="This debug-level line should show up in the output." \n$`),
		},
		{
			func() {
				log.Debugln("This debug-level line should not show up in the output.")
				log.Infof("This %s-level line should not show up in the output.", "info")
				log.Errorf("This %s-level line should show up in the output.", "error")
			},
			logrus.ErrorLevel,
			regexp.MustCompile(`^time=".*" level=error msg="This error-level line should show up in the output." \n$`),
		},
		{
			func() {
				log.Errorln("This error-level line should show up in the output.")
				log.Infoln("This info-level line should not show up in the output.")
			},
			logrus.ErrorLevel,
			regexp.MustCompile(`^time=".*" level=error msg="This error-level line should show up in the output." \n$`),
		},
		{
			func() {
				log.With("key", "value").Infoln("This info-level line should show up in the output.")
			},
			logrus.InfoLevel,
			regexp.MustCompile(`^time=".*" level=info msg="This info-level line should show up in the output." key=value \n$`),
		},
		{
			func() {
				log.Base().Output(0, "This info-level line should show up in the output.")
			},
			logrus.InfoLevel,
			regexp.MustCompile(`^time=".*" level=info msg="This info-level line should show up in the output." \n$`),
		},
	}
)

func TestFileLineLogging(t *testing.T) {
	for _, lt := range loggerTests {
		var buf bytes.Buffer
		log.Orig().Out = &buf
		log.Orig().Level = lt.logLevel
		log.Orig().Formatter = &logrus.TextFormatter{
			DisableColors: true,
		}

		lt.logLines()

		if !lt.re.Match(buf.Bytes()) {
			t.Fatalf("%q did not match expected regex %q", buf.String(), lt.re.String())
		}
	}
}

func TestCommandLineFlag(t *testing.T) {
	if err := flag.Set("log.level", "error"); err != nil {
		t.Fatalf("unexpected flag.Set error, %s", err)
	}
	flag.Parse()
}

func TestCommandLineFlagErr(t *testing.T) {
	if err := flag.Set("log.level", "erro"); err == nil {
		t.Fatal("expected flag.Set error, none received")
	}
	flag.Parse()
}
