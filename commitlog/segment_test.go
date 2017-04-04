package commitlog_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/compose/transporter/commitlog"
)

var (
	path         = filepath.Join(os.TempDir(), fmt.Sprintf("newsegmenttest%d", rand.Int63()))
	segmentTests = []struct {
		p        string
		offset   int64
		expected *commitlog.Segment
		filename string
	}{
		{
			path,
			0,
			&commitlog.Segment{
				BaseOffset: 0,
				NextOffset: 0,
				Position:   0,
			},
			"00000000000000000000.log",
		},
		{
			path,
			100,
			&commitlog.Segment{
				BaseOffset: 100,
				NextOffset: 100,
				Position:   0,
			},
			"00000000000000000100.log",
		},
		{
			"testdata",
			0,
			&commitlog.Segment{
				BaseOffset: 0,
				NextOffset: 1,
				Position:   37,
			},
			"00000000000000000000.log",
		},
	}
)

func TestNewSegment(t *testing.T) {
	setup(path, t)
	defer cleanup(path, t)
	for _, st := range segmentTests {
		actualS, err := commitlog.NewSegment(st.p, st.offset, 1024)
		if err != nil {
			t.Fatalf("unexpected NewSegment error, %s", err)
		}
		if !reflect.DeepEqual(st.expected.BaseOffset, actualS.BaseOffset) {
			t.Errorf("wrong BaseOffset, expected %d, got %d", st.expected.BaseOffset, actualS.BaseOffset)
		}
		if !reflect.DeepEqual(st.expected.NextOffset, actualS.NextOffset) {
			t.Errorf("wrong NextOffset, expected %d, got %d", st.expected.NextOffset, actualS.NextOffset)
		}
		if !reflect.DeepEqual(st.expected.Position, actualS.Position) {
			t.Errorf("wrong Position, expected %d, got %d", st.expected.Position, actualS.Position)
		}
		if _, err := os.Stat(filepath.Join(path, st.filename)); err != nil {
			t.Errorf("unexpected os.Stat err, %s", err)
		}
	}
}

var (
	writePath = filepath.Join(os.TempDir(), fmt.Sprintf("writesegmenttest%d", rand.Int63()))
)

func TestWrite(t *testing.T) {
	setup(writePath, t)
	defer cleanup(writePath, t)
	s, err := commitlog.NewSegment(writePath, 0, 1024)
	if err != nil {
		t.Fatalf("unexpected NewSegment error, %s", err)
	}
	n, err := s.Write(entryTests[0].expectedLog)
	if err != nil {
		t.Fatalf("unexpected Write error, %s", err)
	}
	if !reflect.DeepEqual(n, int(s.Position)) {
		t.Errorf("Write return and Segment Position mismatched, expected %d, got %d", s.Position, n)
	}
	if !reflect.DeepEqual(1, int(s.NextOffset)) {
		t.Errorf("wrong NextOffset, expected %d, got %d", 1, s.NextOffset)
	}
}

var (
	fullPath = filepath.Join(os.TempDir(), fmt.Sprintf("fullsegmenttest%d", rand.Int63()))
)

func TestIsFull(t *testing.T) {
	setup(fullPath, t)
	defer cleanup(fullPath, t)
	s, err := commitlog.NewSegment(fullPath, 0, 10)
	if err != nil {
		t.Fatalf("unexpected NewSegment error, %s", err)
	}
	if s.IsFull() {
		t.Errorf("IsFull was true but expected false")
	}
	if _, err := s.Write(entryTests[0].expectedLog); err != nil {
		t.Fatalf("unexpected Write error, %s", err)
	}
	if !s.IsFull() {
		t.Errorf("IsFull was false but expected true")
	}
}

var (
	closePath = filepath.Join(os.TempDir(), fmt.Sprintf("closesegmenttest%d", rand.Int63()))
)

func TestClose(t *testing.T) {
	setup(closePath, t)
	defer cleanup(closePath, t)
	s, err := commitlog.NewSegment(closePath, 0, 1024)
	if err != nil {
		t.Fatalf("unexpected NewSegment error, %s", err)
	}
	if _, err := s.Write(entryTests[0].expectedLog); err != nil {
		t.Fatalf("unexpected Write error, %s", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("unexpected Close error, %s", err)
	}
}

func setup(p string, t *testing.T) {
	err := os.MkdirAll(p, 0755)
	if err != nil {
		t.Fatalf("mkdir (%s) failed, %s", path, err)
	}
}

func cleanup(p string, t *testing.T) {
	os.RemoveAll(p)
}
