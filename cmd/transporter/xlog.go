package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/compose/transporter/commitlog"
	"github.com/compose/transporter/log"
)

func runXlog(args []string) error {
	flagset := baseFlagSet("xlog")
	logDir := flagset.String("log_dir", "", "path to commit log directory")
	flagset.Usage = usageFor(flagset, "transporter xlog --log_dir=/path/to/log oldest|current|show [OFFSET]")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	if *logDir == "" {
		return errors.New("missing required flag --log_dir")
	}

	args = flagset.Args()
	if len(args) <= 0 {
		return errors.New("missing subcommand oldest|current|show")
	}

	log.Orig().Out = ioutil.Discard

	l, err := commitlog.New(commitlog.WithPath(*logDir))
	if err != nil {
		return err
	}

	switch args[0] {
	case "oldest":
		fmt.Println(l.OldestOffset())
	case "current":
		fmt.Println(l.NewestOffset() - 1)
	case "show":
		if len(args) < 2 {
			return errors.New("missing offset argment")
		}
		offset, err := strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("invalid offset provided, %s", args[1])
		}
		r, err := l.NewReader(int64(offset))
		if err != nil {
			return err
		}
		_, e, err := commitlog.ReadEntry(r)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "%-10s: %d\n", "offset", offset)
		ts := time.Unix(int64(e.Timestamp), 0)
		fmt.Fprintf(os.Stdout, "%-10s: %s\n", "timestamp", ts)
		fmt.Fprintf(os.Stdout, "%-10s: %s\n", "mode", e.Mode.String())
		fmt.Fprintf(os.Stdout, "%-10s: %s\n", "op", strings.ToUpper(e.Op.String()))
		fmt.Fprintf(os.Stdout, "%-10s: %s\n", "key", string(e.Key))
		fmt.Fprintf(os.Stdout, "%-10s: %s\n", "value", string(e.Value))
	}

	return nil
}
