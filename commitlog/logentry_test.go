package commitlog_test

import (
	"bytes"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/compose/transporter/commitlog"
	"github.com/compose/transporter/message/ops"
)

var (
	entryTests = []struct {
		name        string
		offset      int64
		le          commitlog.LogEntry
		expectedLog commitlog.Log
	}{
		{
			"base",
			0,
			commitlog.LogEntry{
				Key:       []byte(`key`),
				Value:     []byte(`value`),
				Timestamp: uint64(1491252302),
			},
			commitlog.Log{
				0, 0, 0, 0, 0, 0, 0, 0, // offset
				0, 0, 0, 16, // size
				0, 0, 0, 0, 88, 226, 180, 78, // timestamp
				0,          // mode
				0, 0, 0, 3, // key length
				107, 101, 121, // key
				0, 0, 0, 5, // value length
				118, 97, 108, 117, 101, // value
			},
		},
		{
			"with_offset",
			100,
			commitlog.LogEntry{
				Key:       []byte(`key`),
				Value:     []byte(`value`),
				Timestamp: uint64(1491252302),
			},
			commitlog.Log{
				0, 0, 0, 0, 0, 0, 0, 100, // offset
				0, 0, 0, 16, // size
				0, 0, 0, 0, 88, 226, 180, 78, // timestamp
				0,          // mode
				0, 0, 0, 3, // key length
				107, 101, 121, // key
				0, 0, 0, 5, // value length
				118, 97, 108, 117, 101, // value
			},
		},
		{
			"with_sync_mode",
			0,
			commitlog.LogEntry{
				Key:       []byte(`key`),
				Value:     []byte(`value`),
				Timestamp: uint64(1491252302),
				Mode:      commitlog.Sync,
			},
			commitlog.Log{
				0, 0, 0, 0, 0, 0, 0, 0, // offset
				0, 0, 0, 16, // size
				0, 0, 0, 0, 88, 226, 180, 78, // timestamp
				1,          // mode
				0, 0, 0, 3, // key length
				107, 101, 121, // key
				0, 0, 0, 5, // value length
				118, 97, 108, 117, 101, // value
			},
		},
		{
			"with_complete_mode",
			0,
			commitlog.LogEntry{
				Key:       []byte(`key`),
				Value:     []byte(`value`),
				Timestamp: uint64(1491252302),
				Mode:      commitlog.Complete,
			},
			commitlog.Log{
				0, 0, 0, 0, 0, 0, 0, 0, // offset
				0, 0, 0, 16, // size
				0, 0, 0, 0, 88, 226, 180, 78, // timestamp
				2,          // mode
				0, 0, 0, 3, // key length
				107, 101, 121, // key
				0, 0, 0, 5, // value length
				118, 97, 108, 117, 101, // value
			},
		},
		{
			"with_update_op",
			0,
			commitlog.LogEntry{
				Key:       []byte(`key`),
				Value:     []byte(`value`),
				Timestamp: uint64(1491252302),
				Op:        ops.Update,
			},
			commitlog.Log{
				0, 0, 0, 0, 0, 0, 0, 0, // offset
				0, 0, 0, 16, // size
				0, 0, 0, 0, 88, 226, 180, 78, // timestamp
				4,          // mode
				0, 0, 0, 3, // key length
				107, 101, 121, // key
				0, 0, 0, 5, // value length
				118, 97, 108, 117, 101, // value
			},
		},
		{
			"with_sync_mode_delete_op",
			0,
			commitlog.LogEntry{
				Key:       []byte(`key`),
				Value:     []byte(`value`),
				Timestamp: uint64(1491252302),
				Mode:      commitlog.Sync,
				Op:        ops.Delete,
			},
			commitlog.Log{
				0, 0, 0, 0, 0, 0, 0, 0, // offset
				0, 0, 0, 16, // size
				0, 0, 0, 0, 88, 226, 180, 78, // timestamp
				9,          // mode
				0, 0, 0, 3, // key length
				107, 101, 121, // key
				0, 0, 0, 5, // value length
				118, 97, 108, 117, 101, // value
			},
		},
	}
)

func TestNewLogFromEntry(t *testing.T) {
	for _, et := range entryTests {
		actualLog := commitlog.NewLogFromEntry(et.le)
		actualLog.PutOffset(et.offset)
		if !reflect.DeepEqual(actualLog, et.expectedLog) {
			t.Errorf("[%s] bad log, expected versus got\n%+v\n%+v", et.name, et.expectedLog, actualLog)
		}
	}
}

var (
	readEntryTests = []struct {
		reader         func(t *testing.T) (io.Reader, func())
		expectedOffset uint64
		expectedEntry  commitlog.LogEntry
		expectedErr    error
	}{
		{
			func(t *testing.T) (io.Reader, func()) {
				log, err := os.OpenFile("testdata/00000000000000000000.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
				if err != nil {
					t.Fatalf("unexpected OpenFile error, %s", err)
				}
				return log, func() { log.Close() }
			},
			0,
			commitlog.LogEntry{
				Key:       []byte("key"),
				Value:     []byte("value"),
				Timestamp: 1491252302,
				Mode:      commitlog.Copy,
				Op:        ops.Insert,
			},
			nil,
		},
		{
			func(t *testing.T) (io.Reader, func()) {
				log, err := os.OpenFile("testdata/emptyfile.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
				if err != nil {
					t.Fatalf("unexpected OpenFile error, %s", err)
				}
				return log, func() { log.Close() }
			},
			0,
			commitlog.LogEntry{},
			io.EOF,
		},
	}
)

func checkEntry(t *testing.T, deferFunc func(), actual commitlog.LogEntry, expected commitlog.LogEntry) {
	defer deferFunc()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("wrong LogEntry, expected %+v, got %+v", expected, actual)
	}
}

func TestReadEntry(t *testing.T) {
	for _, ret := range readEntryTests {
		r, d := ret.reader(t)
		offset, entry, err := commitlog.ReadEntry(r)
		if err != ret.expectedErr {
			t.Fatalf("wrong ReadEntry error, expected %s, got %s", ret.expectedErr, err)
		}
		if ret.expectedErr == nil {
			if !reflect.DeepEqual(ret.expectedOffset, offset) {
				t.Errorf("wrong offset, expected %d, got %d", ret.expectedOffset, offset)
			}
			checkEntry(t, d, entry, ret.expectedEntry)
		}
	}
}

var (
	mockEntryTests = []struct {
		name           string
		r              io.Reader
		expectedOffset uint64
		expectedEntry  commitlog.LogEntry
		expectedErr    error
	}{
		{
			"sync",
			bytes.NewBuffer(commitlog.Log{
				0, 0, 0, 0, 0, 0, 0, 0, // offset
				0, 0, 0, 16, // size
				0, 0, 0, 0, 88, 226, 180, 78, // timestamp
				1,          // mode
				0, 0, 0, 3, // key length
				107, 101, 121, // key
				0, 0, 0, 5, // value length
				118, 97, 108, 117, 101, // value
			}),
			0,
			commitlog.LogEntry{
				Key:       []byte("key"),
				Value:     []byte("value"),
				Mode:      commitlog.Sync,
				Op:        ops.Insert,
				Timestamp: 1491252302,
			},
			nil,
		},
		{
			"complete",
			bytes.NewBuffer(commitlog.Log{
				0, 0, 0, 0, 0, 0, 0, 0, // offset
				0, 0, 0, 16, // size
				0, 0, 0, 0, 88, 226, 180, 78, // timestamp
				2,          // mode
				0, 0, 0, 3, // key length
				107, 101, 121, // key
				0, 0, 0, 5, // value length
				118, 97, 108, 117, 101, // value
			}),
			0,
			commitlog.LogEntry{
				Key:       []byte("key"),
				Value:     []byte("value"),
				Mode:      commitlog.Complete,
				Op:        ops.Insert,
				Timestamp: 1491252302,
			},
			nil,
		},
		{
			"mode_with_op",
			bytes.NewBuffer(commitlog.Log{
				0, 0, 0, 0, 0, 0, 0, 0, // offset
				0, 0, 0, 16, // size
				0, 0, 0, 0, 88, 226, 180, 78, // timestamp
				9,          // mode
				0, 0, 0, 3, // key length
				107, 101, 121, // key
				0, 0, 0, 5, // value length
				118, 97, 108, 117, 101, // value
			}),
			0,
			commitlog.LogEntry{
				Key:       []byte("key"),
				Value:     []byte("value"),
				Mode:      commitlog.Sync,
				Op:        ops.Delete,
				Timestamp: 1491252302,
			},
			nil,
		},
		{
			"update",
			bytes.NewBuffer(commitlog.Log{
				0, 0, 0, 0, 0, 0, 0, 100, // offset
				0, 0, 0, 16, // size
				0, 0, 0, 0, 88, 226, 180, 78, // timestamp
				4,          // mode
				0, 0, 0, 3, // key length
				107, 101, 121, // key
				0, 0, 0, 5, // value length
				118, 97, 108, 117, 101, // value
			}),
			100,
			commitlog.LogEntry{
				Key:       []byte("key"),
				Value:     []byte("value"),
				Mode:      commitlog.Copy,
				Op:        ops.Update,
				Timestamp: 1491252302,
			},
			nil,
		},
		{
			"with_err",
			bytes.NewBuffer(commitlog.Log{
				0, 0, 0, 0, 0, 0, 0, 100, // offset
				0, 0, 0, 16, // size
				0, 0, 0, 0, 88, 226, 180, 78, // timestamp
				4, // mode
			}),
			100,
			commitlog.LogEntry{},
			io.EOF,
		},
	}
)

func TestReadEntryMock(t *testing.T) {
	for _, met := range mockEntryTests {
		offset, entry, err := commitlog.ReadEntry(met.r)
		if err != met.expectedErr {
			t.Fatalf("[%s] wrong ReadEntry error, expected %s, got %s", met.name, met.expectedErr, err)
		}
		if met.expectedErr == nil {
			if !reflect.DeepEqual(met.expectedOffset, offset) {
				t.Errorf("[%s] wrong offset, expected %d, got %d", met.name, met.expectedOffset, offset)
			}
			checkEntry(t, func() {}, entry, met.expectedEntry)
		}
	}
}

func BenchmarkNewLogFromEntry(b *testing.B) {
	le := entryTests[0].le
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		commitlog.NewLogFromEntry(le)
	}
}
