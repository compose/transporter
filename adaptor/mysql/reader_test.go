package mysql

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/message"
)

var (
	readerTestData = &TestData{"reader_test", "reader_test_table", basicSchema, 10}
)

func TestRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Read in short mode")
	}

	reader := newReader()
	readFunc := reader.Read(map[string]client.MessageSet{}, func(table string) bool {
		if strings.HasPrefix(table, "information_schema.") || strings.HasPrefix(table, "performance_schema.") {
			return false
		}
		return table == readerTestData.DB + "." + readerTestData.Table
	})
	done := make(chan struct{})
	c, err := NewClient(WithURI(fmt.Sprintf("mysql://root@tcp(localhost)/%s", readerTestData.DB)))
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
		if strings.HasPrefix(table, "information_schema.") || strings.HasPrefix(table, "performance_schema."){
			return false
		}
		return table == readerComplexTestData.DB + "." + readerComplexTestData.Table
	})
	done := make(chan struct{})
	c, err := NewClient(WithURI(fmt.Sprintf("mysql://root@tcp(localhost)/%s", readerComplexTestData.DB)))
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
			"id":                 int64(i),
			"colvar":             randomHeros[i%len(randomHeros)],
			"colbigint":          int64(4000001240124),
			"colbit":             "1",
			"colboolean":         false,
			"colbinary":          0xDEADBEEF,
			"colcharacter":       "a",
			"coldoubleprecision": 0.314259892323,
			"colenum":            "sad",
			"colinteger":         int64(3),
			"colline":            "{1,1,3}",
			"collseg":            "[(10,10),(25,25)]",
			"colmacaddr":         "08:00:2b:01:02:03",
			"colmoney":           35.68,
			"colnumeric":         0.23509838,
			"colpath":            "[(10,10),(20,20),(20,10),(15,15)]",
			"colpg_lsn":          "0/3000000",
			"colpoint":           "(15,15)",
			"colpolygon":         "((10,10),(11,11),(11,0),(5,5))",
			"colreal":            float64(7),
			"colsmallint":        int64(3),
			"coltext":            "this is \\n extremely important",
			"coltime":            time.Date(0, 1, 1, 13, 45, 0, 0, time.UTC),
			"coltsquery":         "'fat':AB & 'cat'",
			"coluuid":            "f0a0da24-4068-4be4-961d-7c295117ccca",
			"colxml":             "<person><name>Batman</name></person>",
		} {
			if msgs[i].Data().Get(key) != value {
				t.Fatalf("Expected %v of row to equal %v (%T), but was %v (%T)", key, value, value, msgs[i].Data().Get(key), msgs[i].Data().Get(key))
			}
		}
	}
	close(done)
}
