package mysql

import (
	_ "embed"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/message"
)

var (
	readerTestData = &TestData{"reader_test", "reader_test_table", basicSchema, 10}

	// For testing Blob
	//go:embed logo-mysql-170x115.png
	blobdata string
)

func TestRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Read in short mode")
	}

	reader := newReader()
	readFunc := reader.Read(map[string]client.MessageSet{}, func(table string) bool {
		if strings.HasPrefix(table, "information_schema.") ||
			strings.HasPrefix(table, "performance_schema.") ||
			strings.HasPrefix(table, "mysql.") ||
			strings.HasPrefix(table, "sys.") {

			return false
		}
		return table == readerTestData.DB+"."+readerTestData.Table
	})
	done := make(chan struct{})
	c, err := NewClient(WithURI(fmt.Sprintf("mysql://root@localhost:3306?%s", readerTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to mysql, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to mysql, %s", err)
	}
	msgChan, err := readFunc(s, done)
	if err != nil {
		t.Fatalf("unexpected Read error, %s\n", err)
	}
	var numMsgs int
	for range msgChan {
		numMsgs++
	}
	if numMsgs != readerTestData.InsertCount {
		t.Errorf("bad message count, expected %d, got %d\n", readerTestData.InsertCount, numMsgs)
	}
	close(done)
}

var (
	readerComplexTestData = &TestData{"reader_complex_test", "reader_complex_test_table", complexSchema, 10}
)

func TestReadComplex(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Read in short mode")
	}

	reader := newReader()
	readFunc := reader.Read(map[string]client.MessageSet{}, func(table string) bool {
		if strings.HasPrefix(table, "information_schema.") ||
			strings.HasPrefix(table, "performance_schema.") ||
			strings.HasPrefix(table, "mysql.") ||
			strings.HasPrefix(table, "sys.") {

			return false
		}
		return table == readerComplexTestData.DB+"."+readerComplexTestData.Table
	})
	done := make(chan struct{})
	c, err := NewClient(WithURI(fmt.Sprintf("mysql://root@localhost:3306?%s", readerComplexTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to mysql, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to mysql, %s", err)
	}
	msgChan, err := readFunc(s, done)
	if err != nil {
		t.Fatalf("unexpected Read error, %s\n", err)
	}
	msgs := make([]message.Msg, 0)
	for msg := range msgChan {
		msgs = append(msgs, msg.Msg)
	}
	if len(msgs) != readerComplexTestData.InsertCount {
		t.Errorf("bad message count, expected %d, got %d\n", readerComplexTestData.InsertCount, len(msgs))
	}
	for i := 0; i < readerTestData.InsertCount; i++ {
		for key, value := range map[string]interface{}{
			"id":                    int64(i) + 1,
			"colinteger":            int64(i),
			"colsmallint":           int64(32767),
			"coltinyint":            int64(127),
			"colmediumint":          int64(8388607),
			"colbigint":             int64(21474836471),
			"coldecimal":            0.23509838,
			"colfloat":              0.31426,
			"coldoubleprecision":    0.314259892323,
			"colbit":                "101",
			"coldate":               time.Date(2021, 12, 10, 0, 0, 0, 0, time.UTC),
			"coltime":               "13:45:00",
			"colyear":               uint64(2021),
			"colchar":               "a",
			"colvar":                randomHeros[i%len(randomHeros)],
			"colbinary":             "deadbeef000000000000",
			"colblob":               blobdata,
			"coltext":               "this is extremely important",
			"coljson":               "{\"name\": \"batman\", \"sidekick\": \"robin\"}",
			"colpoint":              "POINT (15 15)",
			"collinestring":         "LINESTRING (0 0, 1 1, 2 2)",
			"colpolygon":            "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (5 5, 7 5, 7 7, 5 7, 5 5))",
			"colgeometrycollection": "GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (0 0, 1 1, 2 2, 3 3, 4 4))",
		} {
			switch {
			case key == "colbinary":
				// NOTE: This is a "hack" for testing purposes.
				// True binary data (colblob) works fine and no additional parsing is required
				// (i.e. nothing in `casifyValue` for it and the blob comparison works)
				// When we insert the Golang value of 0xDEADBEEF into MySQL and just read it we
				// get a weird string. I.e. the actual binary data. But I cannot for the life
				// of me figure out how in Golang to convert 0xDEADBEEF to the same form. I.e.
				// like the blobdata. So...
				//
				// In a mysql shell you can get a human readable form from:
				//
				// mysql> select hex(colbinary) from reader_complex_test_table limit 1;
				// +----------------------+
				// | hex(colbinary)       |
				// +----------------------+
				// | DEADBEEF000000000000 |
				// +----------------------+
				//
				// So that is what we do here just for the ease of testing
				binvalue := hex.EncodeToString([]byte(msgs[i].Data().Get(key).(string)))
				if binvalue != value {
					t.Errorf("Expected %v of row to equal %v (%T), but was %v (%T)", key, value, value, binvalue, binvalue)
				}
			default:
				if msgs[i].Data().Get(key) != value {
					// Fatalf here hides other errors because it's a FailNow so use Error instead
					t.Errorf("Expected %v of row to equal %v (%T), but was %v (%T)", key, value, value, msgs[i].Data().Get(key), msgs[i].Data().Get(key))
				}
			}
		}
	}
	close(done)
}
