package commitlog_test

import (
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

func TestReadHeader(t *testing.T) {
	log, err := os.OpenFile("testdata/00000000000000000000.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		t.Fatalf("unexpected OpenFile error, %s", err)
	}
	defer log.Close()
	offset, size, ts, mode, op, err := commitlog.ReadHeader(log)
	if err != nil {
		t.Fatalf("unexpected ReadHeader error, %s", err)
	}
	if offset != 0 {
		t.Errorf("wrong offset, expected 0, got %d", offset)
	}
	if size != 16 {
		t.Errorf("wrong size, expected 16, got %d", size)
	}
	if ts != 1491252302 {
		t.Errorf("wrong timestamp, expected 1491252302, got %d", ts)
	}
	if mode != commitlog.Copy {
		t.Errorf("wrong mode, expected %d, got %d", commitlog.Copy, mode)
	}
	if op != ops.Insert {
		t.Errorf("wrong op, expected %d, got %d", ops.Insert, op)
	}
}

func TestReadHeaderErr(t *testing.T) {
	log, err := os.OpenFile("testdata/emptyfile.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		t.Fatalf("unexpected OpenFile error, %s", err)
	}
	defer log.Close()
	_, _, _, _, _, err = commitlog.ReadHeader(log)
	if err != io.EOF {
		t.Errorf("wrong error, expected %s, got %s", io.EOF, err)
	}
}

func TestReadKeyValue(t *testing.T) {
	log, err := os.OpenFile("testdata/00000000000000000000.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		t.Fatalf("unexpected OpenFile error, %s", err)
	}
	defer log.Close()
	_, size, _, _, _, err := commitlog.ReadHeader(log)
	if err != nil {
		t.Fatalf("unexpected ReadHeader error, %s", err)
	}
	key, value, err := commitlog.ReadKeyValue(size, log)
	if err != nil {
		t.Fatalf("unexpected ReadKeyValue error, %s", err)
	}
	if string(key) != "key" {
		t.Errorf("wrong key, expected key, got %s", string(key))
	}
	if string(value) != "value" {
		t.Errorf("wrong value, expected value, got %s", string(value))
	}
}

func TestReadKeyValueErr(t *testing.T) {
	log, err := os.OpenFile("testdata/emptyfile.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		t.Fatalf("unexpected OpenFile error, %s", err)
	}
	defer log.Close()
	_, _, err = commitlog.ReadKeyValue(10, log)
	if err != io.EOF {
		t.Errorf("wrong error, expected %s, got %s", io.EOF, err)
	}
}

var (
	modeTests = []struct {
		name         string
		l            commitlog.Log
		expectedMode commitlog.Mode
	}{
		{
			"base",
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
			commitlog.Copy,
		},
		{
			"sync",
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
			commitlog.Sync,
		},
		{
			"complete",
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
			commitlog.Complete,
		},
		{
			"mode_with_op",
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
			commitlog.Sync,
		},
	}
)

func TestModeFromBytes(t *testing.T) {
	for _, mt := range modeTests {
		actualMode := commitlog.ModeFromBytes(mt.l)
		if !reflect.DeepEqual(actualMode, mt.expectedMode) {
			t.Errorf("[%s] wrong Mode, expected %+v, got %+v", mt.name, mt.expectedMode, actualMode)
		}
	}
}

var (
	opTests = []struct {
		name       string
		l          commitlog.Log
		expectedOp ops.Op
	}{
		{
			"base",
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
			ops.Insert,
		},
		{
			"update",
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
			ops.Update,
		},
		{
			"op_with_mode",
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
			ops.Delete,
		},
	}
)

func TestOpFromBytes(t *testing.T) {
	for _, ot := range opTests {
		actualOp := commitlog.OpFromBytes(ot.l)
		if !reflect.DeepEqual(actualOp, ot.expectedOp) {
			t.Errorf("[%s] wrong Op, expected %+v, got %+v", ot.name, ot.expectedOp, actualOp)
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
