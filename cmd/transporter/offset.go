package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/compose/transporter/log"
	"github.com/compose/transporter/offset"
	"github.com/olekukonko/tablewriter"
)

const (
	consumerDirPrefix = "__consumer_offsets-"
)

func runOffset(args []string) error {
	flagset := baseFlagSet("offset")
	logDir := flagset.String("xlog_dir", "", "path to commit log directory")
	flagset.Usage = usageFor(flagset, "transporter offset --xlog_dir=/path/to/log list|show|mark|delete [SINK] [OFFSET]")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	if *logDir == "" {
		return errors.New("missing required flag --xlog_dir")
	}

	args = flagset.Args()
	if len(args) <= 0 {
		return errors.New("missing subcommand list|show|mark|delete")
	}

	log.Orig().Out = ioutil.Discard

	switch args[0] {
	case "list":
		files, err := ioutil.ReadDir(*logDir)
		if err != nil {
			return errors.New("unable to list directory")
		}
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"sink", "offset"})

		for _, file := range files {
			if file.IsDir() && strings.HasPrefix(file.Name(), consumerDirPrefix) {
				name := strings.TrimPrefix(file.Name(), consumerDirPrefix)
				om, _ := offset.NewLogManager(*logDir, name)
				table.Append([]string{name, strconv.Itoa(int(om.NewestOffset()))})
			}
		}
		table.Render()
	case "show":
		sinkName := args[1]
		om, _ := offset.NewLogManager(*logDir, sinkName)
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"namespace", "offset"})
		for ns, nsOffset := range om.OffsetMap() {
			table.Append([]string{ns, strconv.Itoa(int(nsOffset))})
		}
		table.Render()
	case "mark":
		if len(args) != 3 {
			return errors.New("wrong number of arguments, expected mark SINK OFFSET")
		}
		sinkName := args[1]
		o, err := strconv.ParseUint(args[2], 10, 64)
		if err != nil {
			return err
		}

		om, err := offset.NewLogManager(*logDir, sinkName)
		if err != nil {
			return err
		}

		tmpOffsets := make([]offset.Offset, 0)
		for ns, nsOffset := range om.OffsetMap() {
			tmpOffsets = append(tmpOffsets, offset.Offset{
				LogOffset: nsOffset,
				Namespace: ns,
				Timestamp: time.Now().Unix(),
			})
		}
		sort.Slice(tmpOffsets, func(i, j int) bool {
			return tmpOffsets[i].LogOffset < tmpOffsets[j].LogOffset
		})

		toKeep := make([]offset.Offset, 0)
		for _, tmpOffset := range tmpOffsets {
			if tmpOffset.LogOffset < o {
				toKeep = append(toKeep, tmpOffset)
				continue
			}
			toKeep = append(toKeep, offset.Offset{
				LogOffset: o,
				Namespace: tmpOffset.Namespace,
				Timestamp: tmpOffset.Timestamp,
			})
			break
		}

		swapOffsetDir := fmt.Sprintf("%s_swap", sinkName)
		om, err = offset.NewLogManager(*logDir, swapOffsetDir)
		if err != nil {
			return err
		}
		sort.Slice(toKeep, func(i, j int) bool {
			return toKeep[i].LogOffset < toKeep[j].LogOffset
		})
		for _, off := range toKeep {
			if err := om.CommitOffset(off, true); err != nil {
				return err
			}
		}

		offsetDir := filepath.Join(*logDir, fmt.Sprintf("%s%s", consumerDirPrefix, sinkName))
		if err := os.RemoveAll(offsetDir); err != nil {
			return err
		}

		replaceDir := filepath.Join(*logDir, fmt.Sprintf("%s%s", consumerDirPrefix, swapOffsetDir))
		if err := os.Rename(replaceDir, offsetDir); err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "OK")
	case "delete":
		offsetDir := filepath.Join(*logDir, fmt.Sprintf("%s%s", consumerDirPrefix, args[1]))
		err := os.RemoveAll(offsetDir)
		if err == nil {
			fmt.Fprintf(os.Stdout, "OK")
		}
		return err
	}

	return nil
}
