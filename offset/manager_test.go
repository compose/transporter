package offset_test

import (
	"errors"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/offset"
)

var (
	mockManagerTests = []struct {
		name                 string
		expectedOffsetMap    map[string]uint64
		expectedNewestOffset int64
	}{
		{
			"writer0",
			make(map[string]uint64),
			-1,
		},
		{
			"resume0",
			map[string]uint64{"namespace0": 87, "namespace1": 47, "namespace2": 59},
			87,
		},
	}
)

func TestNewMockManager(t *testing.T) {
	for _, mt := range mockManagerTests {
		m := offset.MockManager{MemoryMap: mt.expectedOffsetMap}
		if !reflect.DeepEqual(m.OffsetMap(), mt.expectedOffsetMap) {
			t.Errorf("bad offsetMap, expected %+v, got %+v", mt.expectedOffsetMap, m.OffsetMap())
		}
		if !reflect.DeepEqual(m.NewestOffset(), mt.expectedNewestOffset) {
			t.Errorf("bad NewestOffset, expected %d, got %d", mt.expectedNewestOffset, m.NewestOffset())
		}
	}
}

var (
	mockCommitTests = []struct {
		om      *offset.MockManager
		mockMap map[string]uint64
	}{
		{
			&offset.MockManager{MemoryMap: map[string]uint64{}},
			map[string]uint64{
				"namespace0": uint64(25),
				"namespace1": uint64(81),
				"namespace2": uint64(40),
			},
		},
		{
			&offset.MockManager{
				MemoryMap:   map[string]uint64{},
				CommitDelay: 100 * time.Millisecond,
			},
			map[string]uint64{
				"namespace0": uint64(85),
				"namespace1": uint64(63),
				"namespace2": uint64(95),
			},
		},
	}
)

func TestMockCommitOffset(t *testing.T) {
	rand.Seed(time.Now().Unix())
	for _, mt := range mockCommitTests {
		for ns, lastOffset := range mt.mockMap {
			for i := 0; i <= int(lastOffset); i++ {
				if err := mt.om.CommitOffset(offset.Offset{
					Namespace: ns,
					LogOffset: uint64(i),
					Timestamp: time.Now().Unix(),
				}, false); err != nil {
					t.Fatalf("unexpected CommitOffset error, %s", err)
				}
			}
			if err := mt.om.CommitOffset(offset.Offset{
				Namespace: ns,
				LogOffset: uint64(rand.Intn(int(lastOffset))),
				Timestamp: time.Now().Unix(),
			}, false); err != nil {
				t.Fatalf("unexpected CommitOffset error, %s", err)
			}
		}

		time.Sleep(500 * time.Millisecond)
		if !reflect.DeepEqual(mt.om.OffsetMap(), mt.mockMap) {
			t.Errorf("bad OffsetMap, expected, %+v, got %+v", mt.mockMap, mt.om.OffsetMap())
		}

		var expectedNewestOffset uint64
		for _, v := range mt.mockMap {
			if expectedNewestOffset < v {
				expectedNewestOffset = v
			}
		}
		if !reflect.DeepEqual(mt.om.NewestOffset(), int64(expectedNewestOffset)) {
			t.Errorf("wrong NewestOffset, expected %d, got %d", expectedNewestOffset, mt.om.NewestOffset())
		}
	}
}

func TestMockCommitErr(t *testing.T) {
	mockErr := errors.New("mock commit err")
	m := offset.MockManager{
		MemoryMap: map[string]uint64{},
		CommitErr: mockErr,
	}
	if err := m.CommitOffset(offset.Offset{
		Namespace: "blah",
		LogOffset: uint64(10),
		Timestamp: time.Now().Unix(),
	}, false); err == nil {
		t.Errorf("no error returned but expected %s", mockErr)
	}
	if m.NewestOffset() != -1 {
		t.Errorf("wrong NewestOffset, expected 0, got %d", m.NewestOffset())
	}
}
