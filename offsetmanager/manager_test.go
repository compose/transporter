package offsetmanager_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/offsetmanager"
)

func init() {
	rand.Seed(time.Now().Unix())
}

var managerTests = []struct {
	name, path        string
	expectedOffsetMap map[string]uint64
	expectedErr       error // expected error
}{
	{
		"writer0",
		filepath.Join(os.TempDir(), fmt.Sprintf("managertest%d", rand.Int63())),
		make(map[string]uint64, 0),
		nil,
	},
	{
		"no_perms",
		"testdata/no_perms_create",
		make(map[string]uint64, 0),
		&os.PathError{Op: "mkdir", Path: "testdata/no_perms_create/__consumer_offsets-no_perms", Err: os.ErrPermission},
	},
	{
		"resume0",
		"testdata",
		map[string]uint64{"namespace0": 69, "namespace1": 27, "namespace2": 48},
		nil,
	},
}

func TestNew(t *testing.T) {
	os.Mkdir("testdata/no_perms_create", 0444)
	defer os.RemoveAll("testdata/no_perms_create")
	for _, mt := range managerTests {
		m, err := offsetmanager.New(mt.path, mt.name)
		if mt.expectedErr != nil && err == nil {
			t.Fatalf("[%s] expected New error but didn't receive one", mt.name)
		}
		if mt.expectedErr != nil && !reflect.DeepEqual(err.Error(), mt.expectedErr.Error()) {
			t.Fatalf("[%s] unexpected New error, expected %+v, got %+v\n", mt.name, mt.expectedErr, err)
		}
		if mt.expectedErr == nil && !reflect.DeepEqual(m.OffsetMap(), mt.expectedOffsetMap) {
			t.Errorf("bad offsetMap, expected %+v, got %+v", mt.expectedOffsetMap, m.OffsetMap())
		}
	}
}

var (
	expectedMap = map[string]uint64{
		"namespace0": uint64(rand.Intn(100)),
		"namespace1": uint64(rand.Intn(100)),
		"namespace2": uint64(rand.Intn(100)),
	}
)

func TestApply(t *testing.T) {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("managertest%d", rand.Int63()))
	defer cleanup(path, t)
	m, err := offsetmanager.New(path, "writer0")
	if err != nil {
		t.Fatalf("unexpected New error, %s", err)
	}

	for ns, lastOffset := range expectedMap {
		for i := 0; i <= int(lastOffset); i++ {
			if err := m.CommitOffset(offsetmanager.Offset{
				Namespace: ns,
				Offset:    uint64(i),
				Timestamp: time.Now().Unix(),
			}); err != nil {
				t.Fatalf("unexpected Apply error, %s", err)
			}
		}
		if err := m.CommitOffset(offsetmanager.Offset{
			Namespace: ns,
			Offset:    uint64(rand.Intn(int(lastOffset))),
			Timestamp: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("unexpected Apply error, %s", err)
		}
	}

	if !reflect.DeepEqual(m.OffsetMap(), expectedMap) {
		t.Errorf("bad OffsetMap, expected, %+v, got %+v", expectedMap, m.OffsetMap())
	}
}

func cleanup(p string, t *testing.T) {
	os.RemoveAll(p)
}
