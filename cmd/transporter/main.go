package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	_ "github.com/compose/transporter/adaptor/all"
	"github.com/compose/transporter/log"
)

const (
	defaultPipelineFile = "pipeline.js"
	defaultConfigFile   = "transporter.yaml"
)

var version = "dev" // set by release script

func usage() {
	fmt.Fprintf(os.Stderr, "USAGE\n")
	fmt.Fprintf(os.Stderr, "  %s <command> [flags]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "COMMANDS\n")
	fmt.Fprintf(os.Stderr, "  list      list all configured nodes\n")
	fmt.Fprintf(os.Stderr, "  run       run pipeline loaded from a file\n")
	fmt.Fprintf(os.Stderr, "  test      display the compiled nodes without starting a pipeline\n")
	fmt.Fprintf(os.Stderr, "  about     show information about available adaptors\n")
	fmt.Fprintf(os.Stderr, "  init      initialize a config and pipeline file based from provided adaptors\n")
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "VERSION\n")
	fmt.Fprintf(os.Stderr, "  %s\n", version)
	fmt.Fprintf(os.Stderr, "\n")
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	var run func([]string) error
	switch strings.ToLower(os.Args[1]) {
	case "list":
		run = runList
	case "run":
		run = runRun
	case "test":
		run = runTest
	case "about":
		run = runAbout
	case "init":
		run = runInit
	default:
		usage()
		os.Exit(1)
	}

	if err := run(os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func baseFlagSet(setName string, cfgFile *string) *flag.FlagSet {
	cmdFlags := flag.NewFlagSet(setName, flag.ExitOnError)
	log.AddFlags(cmdFlags)
	if cfgFile != nil {
		cmdFlags.StringVar(cfgFile, "config", defaultConfigFile, "YAML config file")
	}
	return cmdFlags
}

func usageFor(fs *flag.FlagSet, short string) func() {
	return func() {
		fmt.Fprintf(os.Stderr, "USAGE\n")
		fmt.Fprintf(os.Stderr, "  %s\n", short)
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "FLAGS\n")
		w := tabwriter.NewWriter(os.Stderr, 0, 2, 2, ' ', 0)
		fs.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "\t-%s %s\t%s\n", f.Name, f.DefValue, f.Usage)
		})
		w.Flush()
		fmt.Fprintf(os.Stderr, "\n")
	}
}
