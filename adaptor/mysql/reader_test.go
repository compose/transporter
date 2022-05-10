package mysql

import (
	_ "embed"
	"encoding/hex"
	//"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/message"
	"github.com/twpayne/go-geom/encoding/wkbhex"
	"github.com/twpayne/go-geom/encoding/wkt"
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
			// Some values need additional parsing.
			// TODO: See what we can do to tidy things up here
			switch {
			case key == "colbinary":
				binvalue := hex.EncodeToString([]byte(msgs[i].Data().Get(key).(string)))
				if binvalue != value {
					t.Errorf("Expected %v of row to equal %v (%T), but was %v (%T)", key, value, value, binvalue, binvalue)
				}
			case key == "colbit":
				// :puke
				bithexvalue := hex.EncodeToString([]byte(msgs[i].Data().Get(key).(string)))
				// NOTE: No error handling on the below since this is a test file
				bitintvalue, _ := strconv.ParseInt(bithexvalue, 10, 64)
				bitvalue := strconv.FormatInt(bitintvalue, 2)
				if bitvalue != value {
					t.Errorf("Expected %v of row to equal %v (%T), but was %v (%T)", key, value, value, bitvalue, bitvalue)
				}
			case key == "colpoint" || key == "collinestring" || key == "colpolygon" || key == "colgeometrycollection":
				// There is no t.Debugf unfortunately so keeping below but commented out
				//t.Logf("DEBUG: %v (%T)", msgs[i].Data().Get(key), msgs[i].Data().Get(key))
				geomhexvalue := hex.EncodeToString([]byte(msgs[i].Data().Get(key).(string)))
				// Strip SRID
				// NOTE: No error handling on the below since this is a test file
				geom, _ := wkbhex.Decode(geomhexvalue[8:])
				wktGeom, _ := wkt.Marshal(geom)
				if wktGeom != value {
					t.Errorf("Expected %v of row to equal %v (%T), but was %v (%T)", key, value, value, wktGeom, wktGeom)
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
