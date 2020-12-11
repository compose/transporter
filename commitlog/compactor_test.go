package commitlog

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

const (
	origDir = "testdata/compactor/orig"
)

func copyDir(t *testing.T, suffixFilter, srcDir, destDir string) {
	directory, _ := os.Open(srcDir)
	logs, _ := directory.Readdir(-1)
	var wg sync.WaitGroup
	wg.Add(len(logs))
	for _, log := range logs {
		if filepath.Ext(log.Name()) != suffixFilter {
			wg.Done()
			continue
		}
		go func(log os.FileInfo, wg *sync.WaitGroup) {
			defer wg.Done()
			src, err := os.Open(filepath.Join(origDir, log.Name()))
			if err != nil {
				panic("unable to open source file, " + err.Error())
			}
			defer src.Close()
			dst, err := os.Create(filepath.Join(destDir, log.Name()))
			if err != nil {
				panic("unable to open destination file, " + err.Error())
			}
			io.Copy(dst, src)
		}(log, &wg)
	}
	wg.Wait()
}

func TestCompact(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "compact")
	if err != nil {
		t.Fatalf("unable to create temp dir, %s", err)
	}
	os.Mkdir(tmpDir, 0777)
	defer os.RemoveAll(tmpDir)
	fmt.Println("copying test data...")
	copyDir(t, logFileSuffix, origDir, tmpDir)
	fmt.Println("test data copy complete")

	l, err := New(WithPath(tmpDir))
	if err != nil {
		t.Fatalf("unable to create commitlog, %s", err)
	}

	c := &namespaceCompactor{log: l}
	segments := l.Segments()
	c.Compact(uint64(l.NewestOffset()+1), segments[0:len(segments)-1])

	files, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("unable to gather stats about testDir, %s", err)
	}
	if len(files) != 2 {
		t.Errorf("wrong number of log files, expected 2, got %d", len(files))
	}
	for _, file := range files {
		if filepath.Ext(file.Name()) != ".log" {
			t.Errorf("wrong file extension, expected .log, got %s", file.Name())
		}
	}
}

func TestRecover_Swap(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "compact_swap_test")
	if err != nil {
		t.Fatalf("unable to create temp dir, %s", err)
	}
	os.Mkdir(tmpDir, 0777)
	defer os.RemoveAll(tmpDir)
	fmt.Println("copying recover data...")
	copyDir(t, logFileSuffix, origDir, tmpDir)
	copyDir(t, cleanedFileSuffix, origDir, tmpDir)
	fmt.Println("recover data copy complete")
	os.Rename(
		filepath.Join(tmpDir, "00000000000000000000.cleaned"),
		filepath.Join(tmpDir, "00000000000000000000.swap"),
	)

	_, err = New(WithPath(tmpDir))
	if err != nil {
		t.Fatalf("unable to create commitlog, %s", err)
	}

	expected, err := ioutil.ReadFile(filepath.Join(origDir, "00000000000000000000.cleaned"))
	if err != nil {
		t.Fatalf("unable to read original log, %s", err)
	}
	actual, err := ioutil.ReadFile(filepath.Join(tmpDir, "00000000000000000000.log"))
	if err != nil {
		t.Fatalf("unable to read swap log, %s", err)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("swap file not replaced properly!")
	}
}

func TestRecover_Deleted(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "compact_deleted_test")
	if err != nil {
		t.Fatalf("unable to create temp dir, %s", err)
	}
	os.Mkdir(tmpDir, 0777)
	defer os.RemoveAll(tmpDir)
	fmt.Println("copying test data...")
	copyDir(t, logFileSuffix, origDir, tmpDir)
	copyDir(t, cleanedFileSuffix, origDir, tmpDir)
	fmt.Println("test data copy complete")
	os.Rename(
		filepath.Join(tmpDir, "00000000000000000000.cleaned"),
		filepath.Join(tmpDir, "00000000000000000000.swap"),
	)
	os.Rename(
		filepath.Join(tmpDir, "00000000000000000000.log"),
		filepath.Join(tmpDir, "00000000000000000000.deleted"),
	)

	_, err = New(WithPath(tmpDir))
	if err != nil {
		t.Fatalf("unable to create commitlog, %s", err)
	}

	expected, err := ioutil.ReadFile(filepath.Join(origDir, "00000000000000000000.cleaned"))
	if err != nil {
		t.Fatalf("unable to read original log, %s", err)
	}
	actual, err := ioutil.ReadFile(filepath.Join(tmpDir, "00000000000000000000.log"))
	if err != nil {
		t.Fatalf("unable to read swap log, %s", err)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("swap file not replaced properly!")
	}
	files, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("unable to gather stats about testDir, %s", err)
	}
	if len(files) != 2 {
		t.Errorf("wrong number of log files, expected 2, got %d", len(files))
	}
}

func TestRecover_Cleaned(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "compact_cleaned_test")
	if err != nil {
		t.Fatalf("unable to create temp dir, %s", err)
	}
	os.Mkdir(tmpDir, 0777)
	defer os.RemoveAll(tmpDir)
	fmt.Println("copying test data...")
	copyDir(t, logFileSuffix, origDir, tmpDir)
	copyDir(t, cleanedFileSuffix, origDir, tmpDir)
	fmt.Println("test data copy complete")

	_, err = New(WithPath(tmpDir))
	if err != nil {
		t.Fatalf("unable to create commitlog, %s", err)
	}

	files, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("unable to gather stats about testDir, %s", err)
	}
	if len(files) != 2 {
		t.Errorf("wrong number of log files, expected 2, got %d", len(files))
	}

	expectedStat, err := os.Stat(filepath.Join(origDir, "00000000000000000000.log"))
	if err != nil {
		t.Fatalf("unable to read log, %s", err)
	}
	actualStat, err := os.Stat(filepath.Join(tmpDir, "00000000000000000000.log"))
	if err != nil {
		t.Fatalf("unable to read log, %s", err)
	}
	if !reflect.DeepEqual(expectedStat.Size(), actualStat.Size()) {
		t.Errorf("file sizes don't match but should, expected %d, got %d", expectedStat.Size(), actualStat.Size())
	}
}
