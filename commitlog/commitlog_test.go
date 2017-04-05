package commitlog_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/commitlog"
)

func init() {
	rand.Seed(time.Now().Unix())
}

var commitlogTests = []struct {
	name                 string
	options              []commitlog.OptionFunc // input
	expectedNewestOffset int64
	expectedOldestOffset int64
	numSegments          int
	expectedErr          error // expected error
	cleanupDir           bool
}{
	{
		"default_commitlog",
		[]commitlog.OptionFunc{},
		0,
		0,
		1,
		nil,
		true,
	},
	{
		"with_path",
		[]commitlog.OptionFunc{
			commitlog.WithPath(filepath.Join(os.TempDir(), fmt.Sprintf("commitlogtest%d", rand.Int63()))),
		},
		0,
		0,
		1,
		nil,
		true,
	},
	{
		"with_max_segment_bytes",
		[]commitlog.OptionFunc{
			commitlog.WithMaxSegmentBytes(100),
		},
		0,
		0,
		1,
		nil,
		true,
	},
	{
		"empty_path",
		[]commitlog.OptionFunc{commitlog.WithPath("")},
		0,
		0,
		0,
		commitlog.ErrEmptyPath,
		true,
	},
	{
		"with_path_existing_segment",
		[]commitlog.OptionFunc{
			commitlog.WithPath("testdata/commitlog_test"),
		},
		2,
		0,
		1,
		nil,
		false,
	},
	{
		"with_path_existing_segments",
		[]commitlog.OptionFunc{
			commitlog.WithPath("testdata/commitlog_multi_test"),
		},
		2,
		0,
		2,
		nil,
		false,
	},
	{
		"no_perms_create_path",
		[]commitlog.OptionFunc{commitlog.WithPath("testdata/no_perms/path")},
		0,
		0,
		0,
		&os.PathError{Op: "mkdir", Path: "testdata/no_perms/path", Err: os.ErrPermission},
		false,
	},
	{
		"no_perms_path",
		[]commitlog.OptionFunc{commitlog.WithPath("testdata/no_perms_file")},
		0,
		0,
		0,
		&os.PathError{Op: "open", Path: "testdata/no_perms_file", Err: os.ErrPermission},
		false,
	},
	{
		"no_perms_create",
		[]commitlog.OptionFunc{commitlog.WithPath("testdata/no_perms_create")},
		0,
		0,
		0,
		&os.PathError{Op: "open", Path: "testdata/no_perms_create/00000000000000000000.log", Err: os.ErrPermission},
		false,
	},
}

func TestNew(t *testing.T) {
	os.Chmod("testdata/no_perms", 0222)
	os.Mkdir("testdata/no_perms_create", 0444)
	os.Chmod("testdata/no_perms_file", 0222)
	defer os.Chmod("testdata/no_perms", 0755)
	defer os.Chmod("testdata/no_perms_file", 0755)
	defer os.RemoveAll("testdata/no_perms_create")
	for _, ct := range commitlogTests {
		c, err := commitlog.New(ct.options...)
		if ct.expectedErr != nil && err == nil {
			t.Fatalf("[%s] expected New error but didn't receive one", ct.name)
		}
		if ct.expectedErr != nil && !reflect.DeepEqual(err.Error(), ct.expectedErr.Error()) {
			t.Fatalf("[%s] unexpected New error, expected %+v, got %+v\n", ct.name, ct.expectedErr, err)
		}
		if err == nil {
			if c.NewestOffset() != ct.expectedNewestOffset {
				t.Errorf("[%s] wrong NewestOffset, expected %d, got %d", ct.name, ct.expectedNewestOffset, c.NewestOffset())
			}
			if c.OldestOffset() != ct.expectedOldestOffset {
				t.Errorf("[%s] wrong OldestOffset, expected %d, got %d", ct.name, ct.expectedOldestOffset, c.OldestOffset())
			}
			if len(c.Segments()) != ct.numSegments {
				t.Errorf("[%s] wrong number of segments, expected %d, got %d\n", ct.name, ct.numSegments, len(c.Segments()))
			}
			if ct.cleanupDir {
				c.DeleteAll()
			}
		}
	}
}

func TestAppend(t *testing.T) {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("commitlogtest%d", rand.Int63()))
	defer cleanup(path, t)
	c, err := commitlog.New(commitlog.WithPath(path))
	if err != nil {
		t.Fatalf("unexpected New error, %s", err)
	}

	n, err := c.Append(commitlog.NewLogFromEntry(entryTests[0].le))
	if err != nil {
		t.Fatalf("unexpected Append error, %s", err)
	}
	if n != 0 {
		t.Errorf("wrong position returned, expected 0, got %d", n)
	}

	n, err = c.Append(commitlog.NewLogFromEntry(entryTests[1].le))
	if err != nil {
		t.Fatalf("unexpected Append error, %s", err)
	}
	if n != 1 {
		t.Errorf("wrong position returned, expected 1, got %d", n)
	}
}

func TestAppendWithSplit(t *testing.T) {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("commitlogsplittest%d", rand.Int63()))
	defer cleanup(path, t)
	c, err := commitlog.New(
		commitlog.WithPath(path),
		commitlog.WithMaxSegmentBytes(int64(len(entryTests[0].expectedLog))),
	)
	if err != nil {
		t.Fatalf("unexpected New error, %s", err)
	}

	n, err := c.Append(commitlog.NewLogFromEntry(entryTests[0].le))
	if err != nil {
		t.Fatalf("unexpected Append error, %s", err)
	}
	if n != 0 {
		t.Errorf("wrong position returned, expected 0, got %d", n)
	}

	n, err = c.Append(commitlog.NewLogFromEntry(entryTests[2].le))
	if err != nil {
		t.Fatalf("unexpected Append error, %s", err)
	}
	if n != 1 {
		t.Errorf("wrong position returned, expected 1, got %d", n)
	}
	if len(c.Segments()) != 2 {
		t.Errorf("wrong number of segments, expected 2, got %d", len(c.Segments()))
	}
}

var (
	benchPath = filepath.Join(os.TempDir(), fmt.Sprintf("commitlogbenchtest%d", rand.Int63()))
)

func BenchmarkCommitLog(b *testing.B) {
	var err error
	defer os.RemoveAll(benchPath)
	c, err := commitlog.New(
		commitlog.WithPath(benchPath),
		commitlog.WithMaxSegmentBytes(4096),
	)
	if err != nil {
		b.Fatalf("unexpected New error, %s", err)
	}

	le := commitlog.NewLogFromEntry(entryTests[0].le)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = c.Append(le); err != nil {
			b.Errorf("unexpected Append error, %s", err)
		}
	}
}
