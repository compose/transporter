package commitlog_test

import (
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

func BenchmarkNewLogFromEntry(b *testing.B) {
	le := entryTests[0].le
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		commitlog.NewLogFromEntry(le)
	}
}
