package offset_test

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/offset"
)

var (
	bytesTests = []struct {
		name     string
		offset   offset.Offset
		expected []byte
	}{
		{
			"base",
			offset.Offset{
				Namespace: "namespace",
				LogOffset: 0,
				Timestamp: int64(1491252302),
			},
			[]byte{
				0, 0, 0, 0, 0, 0, 0, 0, // offset
				0, 0, 0, 25, // size
				0, 0, 0, 0, 88, 226, 180, 78, // timestamp
				0,          // mode
				0, 0, 0, 9, // key length
				110, 97, 109, 101, 115, 112, 97, 99, 101, // key
				0, 0, 0, 8, // value length
				0, 0, 0, 0, 0, 0, 0, 0, // offset
			},
		},
		{
			"offset_100",
			offset.Offset{
				Namespace: "ns",
				LogOffset: 100,
				Timestamp: int64(1491252302),
			},
			[]byte{
				0, 0, 0, 0, 0, 0, 0, 0, // offset
				0, 0, 0, 18, // size
				0, 0, 0, 0, 88, 226, 180, 78, // timestamp
				0,          // mode
				0, 0, 0, 2, // key length
				110, 115, // key
				0, 0, 0, 8, // value length
				0, 0, 0, 0, 0, 0, 0, 100, // offset
			},
		},
	}
)

func TestBytes(t *testing.T) {
	for _, bt := range bytesTests {
		actual := bt.offset.Bytes()
		if !reflect.DeepEqual(bt.expected, actual) {
			t.Errorf("[%s] bad offset, expected versus got\n%+v\n%+v", bt.name, bt.expected, actual)
		}
	}
}

func BenchmarkOffsetToBytes(b *testing.B) {
	t := time.Now().Unix()
	rand.Seed(t)
	o := offset.Offset{
		Namespace: "namespace",
		LogOffset: uint64(rand.Int63()),
		Timestamp: t,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o.Bytes()
	}
}
