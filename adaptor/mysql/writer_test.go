package mysql

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkt"
)

var optests = []struct {
	op         ops.Op
	registered bool
}{
	{ops.Insert, true},
	{ops.Update, true},
	{ops.Delete, true},
	{ops.Command, false},
	{ops.Noop, false},
}

func TestOpFunc(t *testing.T) {
	w := newWriter()
	for _, ot := range optests {
		if _, ok := w.writeMap[ot.op]; ok != ot.registered {
			t.Errorf("op (%s) registration incorrect, expected %+v, got %+v\n", ot.op.String(), ot.registered, ok)
		}
	}
}

var (
	writerTestData = &TestData{"writer_insert_test", "simple_test_table", basicSchema, 0}
)

func TestInsert(t *testing.T) {
	confirms, cleanup := adaptor.MockConfirmWrites()
	defer adaptor.VerifyWriteConfirmed(cleanup, t)
	w := newWriter()
	c, err := NewClient(WithURI(fmt.Sprintf("mysql://root@localhost:3306?%s", writerTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to mysql, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to mysql, %s", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := w.Write(
			message.WithConfirms(
				confirms,
				message.From(
					ops.Insert,
					fmt.Sprintf("%s.%s", writerTestData.DB, writerTestData.Table),
					data.Data{"id": i, "colvar": "hello world", "coltimestamp": time.Now().UTC()}),
			),
		)(s); err != nil {
			t.Errorf("unexpected Insert error, %s\n", err)
		}
	}

	if _, err := w.Write(message.WithConfirms(
		confirms,
		message.From(
			ops.Command,
			fmt.Sprintf("%s.%s", writerTestData.DB, writerTestData.Table),
			map[string]interface{}{},
		)),
	)(s); err != nil {
		t.Errorf("unexpected Command error, %s", err)
	}

	if _, err := w.Write(message.From(
		ops.Command,
		fmt.Sprintf("%s.%s", writerTestData.DB, writerTestData.Table),
		map[string]interface{}{},
	))(s); err != nil {
		t.Errorf("unexpected Command error, %s", err)
	}

	var (
		id          int
		stringValue string
		timeValue   time.Time
	)
	if err := s.(*Session).mysqlSession.
		QueryRow(fmt.Sprintf("SELECT id, colvar, coltimestamp FROM %s WHERE id = 4", writerTestData.Table)).
		Scan(&id, &stringValue, &timeValue); err != nil {
		t.Fatalf("Error on test query: %v", err)
	}
	if id != 4 || stringValue != "hello world" || timeValue.Before(time.Now().Add(-30*time.Second).UTC()) {
		t.Fatalf("Values were not what they were expected to be: %v, %v, %v", id, stringValue, timeValue)
	}

	var count int
	err = s.(*Session).mysqlSession.
		QueryRow(fmt.Sprintf("SELECT COUNT(id) FROM %s;", writerTestData.Table)).
		Scan(&count)
	if err != nil {
		t.Errorf("unable to count table, %s", err)
	}
	if count != 10 {
		t.Errorf("wrong document count, expected 10, got %d", count)
	}
}

var (
	writerComplexTestData = &TestData{"writer_complex_insert_test", "complex_test_table", complexSchema, 0}
)


func wktToGeom(wktForm string) geom.T {
    // TODO: Handle errors!!
    geomForm, _ := wkt.Unmarshal(wktForm)
    return geomForm
}


func TestComplexInsert(t *testing.T) {
	w := newWriter()
	c, err := NewClient(WithURI(fmt.Sprintf("mysql://root@localhost:3306?%s", writerComplexTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to mysql, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to mysql, %s", err)
	}
	// These need to be Go native?
	// What creates this table? Because we need to match...
	// !! This has to match `complex_schema` in adaptor_test !!
	for i := 0; i < 10; i++ {
		msg := message.From(ops.Insert, fmt.Sprintf("%s.%s", writerComplexTestData.DB, writerComplexTestData.Table), data.Data{
			"id":                    i,
			"colinteger":            int64(3),
			"colsmallint":           int64(32767),
			"coltinyint":            int64(127),
			"colmediumint":          int64(8388607),
			"colbigint":             int64(21474836471),
			"coldecimal":            0.23509838,
			"colfloat":              0.31426,
			"coldoubleprecision":    0.314259892323,
			// I think we need to do what we did in reader_test, but in reverse?
			// "b'101'" gets interpreted as a string
			"colbit":                0b101,
			"coldate":               time.Date(2021, 12, 10, 0, 0, 0, 0, time.UTC),
			"coltime":               "13:45:00",
			"coltimestamp":          time.Now().UTC(),
			"colyear":               "2021",
			"colchar":               "a",
			"colvar":                randomHeros[i],
			"colbinary":             0xDEADBEEF,
			"colblob":               0xDEADBEEF,
			"coltext":               "this is extremely important",
			// Maybe it makes sense to have geometry as a Go representation of geometry
			// So go-geom since we are using that at the moment
			// And then we can manipulate in writer.go to insert as required
			"colpoint":              wktToGeom("POINT (15 15)"),
			"collinestring":         wktToGeom("LINESTRING (0 0, 1 1, 2 2)"),
			"colpolygon":            wktToGeom("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 7 5, 7 7, 5 7, 5 5))"),
			"colgeometrycollection": wktToGeom("GEOMETRYCOLLECTION (POINT (1 1),LINESTRING (0 0, 1 1, 2 2, 3 3, 4 4))"),
		})
		if _, err := w.Write(msg)(s); err != nil {
			t.Errorf("unexpected Insert error, %s\n", err)
		}
	}
	var (
		id          int
		stringValue string
		timeValue   time.Time
	)
	if err := s.(*Session).mysqlSession.
		QueryRow(fmt.Sprintf("SELECT id, colvar, coltimestamp FROM %s WHERE id = 4", writerComplexTestData.Table)).
		Scan(&id, &stringValue, &timeValue); err != nil {
		t.Fatalf("Error on test query: %v", err)
	}
	if id != 4 || stringValue != randomHeros[4] || timeValue.Before(time.Now().Add(-30*time.Second).UTC()) {
		t.Fatalf("Values were not what they were expected to be: %v, %v, %v", id, stringValue, timeValue)
	}

	var count int
	err = s.(*Session).mysqlSession.
		QueryRow(fmt.Sprintf("SELECT COUNT(id) FROM %s;", writerComplexTestData.Table)).
		Scan(&count)
	if err != nil {
		t.Errorf("unable to count table, %s", err)
	}
	if count != 10 {
		t.Errorf("wrong document count, expected 10, got %d", count)
	}
}

var (
	writerUpdateTestData = &TestData{"writer_update_test", "update_test_table", basicSchema, 0}
)

func TestUpdate(t *testing.T) {
	w := newWriter()
	c, err := NewClient(WithURI(fmt.Sprintf("mysql://root@localhost:3306?%s", writerUpdateTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to mysql, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to mysql, %s", err)
	}
	for i := 0; i < 10; i++ {
		msg := message.From(
			ops.Insert,
			fmt.Sprintf("%s.%s", writerUpdateTestData.DB, writerUpdateTestData.Table),
			data.Data{"id": i, "colvar": "hello world", "coltimestamp": time.Now().UTC()})
		if _, err := w.Write(msg)(s); err != nil {
			t.Errorf("unexpected Insert error, %s\n", err)
		}
	}
	msg := message.From(
		ops.Update,
		fmt.Sprintf("%s.%s", writerUpdateTestData.DB, writerUpdateTestData.Table),
		data.Data{"id": 1, "colvar": "robin", "coltimestamp": time.Now().UTC()})
	if _, err := w.Write(msg)(s); err != nil {
		t.Errorf("unexpected Update error, %s\n", err)
	}

	var (
		id          int
		stringValue string
		timeValue   time.Time
	)
	if err := s.(*Session).mysqlSession.
		QueryRow(fmt.Sprintf("SELECT id, colvar, coltimestamp FROM %s WHERE id = 1", writerUpdateTestData.Table)).
		Scan(&id, &stringValue, &timeValue); err != nil {
		t.Fatalf("Error on test query: %v", err)
	}
	if id != 1 || stringValue != "robin" || timeValue.Before(time.Now().Add(-30*time.Second).UTC()) {
		t.Fatalf("Values were not what they were expected to be: %v, %v, %v", id, stringValue, timeValue)
	}

	var count int
	err = s.(*Session).mysqlSession.
		QueryRow(fmt.Sprintf("SELECT COUNT(id) FROM %s;", writerUpdateTestData.Table)).
		Scan(&count)
	if err != nil {
		t.Errorf("unable to count table, %s", err)
	}
	if count != 10 {
		t.Errorf("wrong document count, expected 10, got %d", count)
	}
}

var (
	writerComplexUpdateTestData = &TestData{"writer_complex_update_test", "complex_update_test_table", complexSchema, 10}
)

func TestComplexUpdate(t *testing.T) {
	ranInt := rand.Intn(writerComplexUpdateTestData.InsertCount)
	w := newWriter()
	c, err := NewClient(WithURI(fmt.Sprintf("mysql://root@localhost:3306?%s", writerComplexUpdateTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to mysql, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to mysql, %s", err)
	}
	msg := message.From(ops.Update, fmt.Sprintf("%s.%s", writerComplexUpdateTestData.DB, writerComplexUpdateTestData.Table), data.Data{
		"id":                    ranInt+1,
		"colinteger":            int64(4),
		"colsmallint":           int64(30000),
		"coltinyint":            int64(100),
		"colmediumint":          int64(8000000),
		"colbigint":             int64(4000001240125),
		"coldecimal":            0.23509838,
		"colfloat":              0.31426,
		"coldoubleprecision":    0.314259892323,
		"colbit":                0b101,
		"coldate":               time.Date(2022, 01, 01, 0, 0, 0, 0, time.UTC),
		"coltime":               "14:45:00",
		"coltimestamp":          time.Now().UTC(),
		"colyear":               "2022",
		"colchar":               "b",
		"colvar":                randomHeros[ranInt],
		"colbinary":             0xCAFEBABE,
		"colblob":               0xCAFEBABE,
		"coltext":               "this is extremely important",
		"colpoint":              wktToGeom("POINT (20 20)"),
		"collinestring":         wktToGeom("LINESTRING (3 3, 4 4, 5 5)"),
		"colpolygon":            wktToGeom("POLYGON ((1 1, 11 1, 11 11, 1 11, 1 1),(6 6, 8 6, 8 8, 6 8, 6 6))"),
		"colgeometrycollection": wktToGeom("GEOMETRYCOLLECTION (POINT (2 2),LINESTRING (5 5, 6 6, 7 7, 8 8, 9 9))"),
	})
	if _, err := w.Write(msg)(s); err != nil {
		t.Errorf("unexpected Update error, %s\n", err)
	}

	var (
		id          int
		stringValue string
		timeValue   time.Time
		bigint      int64
	)
	if err := s.(*Session).mysqlSession.
		QueryRow(fmt.Sprintf("SELECT id, colvar, coltimestamp, colbigint FROM %s WHERE id = %d", writerComplexUpdateTestData.Table, ranInt+1)).
		Scan(&id, &stringValue, &timeValue, &bigint); err != nil {
		t.Fatalf("Error on test query: %v", err)
	}
	
	if id != ranInt+1 || stringValue != randomHeros[ranInt] || timeValue.Before(time.Now().Add(-30*time.Second).UTC()) || bigint != int64(4000001240125) {
		t.Fatalf("Values were not what they were expected to be: %v, %v, %v, %v", id, stringValue, timeValue, bigint)
	}

	var count int
	err = s.(*Session).mysqlSession.
		QueryRow(fmt.Sprintf("SELECT COUNT(id) FROM %s;", writerComplexUpdateTestData.Table)).
		Scan(&count)
	if err != nil {
		t.Errorf("unable to count table, %s", err)
	}
	if count != writerComplexUpdateTestData.InsertCount {
		t.Errorf("wrong document count, expected %d, got %d", writerComplexUpdateTestData.InsertCount, count)
	}
}

var (
	writerDeleteTestData = &TestData{"writer_delete_test", "delete_test_table", basicSchema, 0}
)

func TestDelete(t *testing.T) {
	w := newWriter()
	c, err := NewClient(WithURI(fmt.Sprintf("mysql://root@localhost:3306?%s", writerDeleteTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to mysql, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to mysql, %s", err)
	}
	for i := 0; i < 10; i++ {
		msg := message.From(
			ops.Insert,
			fmt.Sprintf("%s.%s", writerDeleteTestData.DB, writerDeleteTestData.Table),
			data.Data{"id": i, "colvar": "hello world", "coltimestamp": time.Now().UTC()})
		if _, err := w.Write(msg)(s); err != nil {
			t.Errorf("unexpected Insert error, %s\n", err)
		}
	}
	msg := message.From(ops.Delete, fmt.Sprintf("%s.%s", writerDeleteTestData.DB, writerDeleteTestData.Table), data.Data{"id": 1})
	if _, err := w.Write(msg)(s); err != nil {
		t.Errorf("unexpected Update error, %s\n", err)
	}

	var id int
	if err := s.(*Session).mysqlSession.
		QueryRow(fmt.Sprintf("SELECT id FROM %s WHERE id = 1", writerDeleteTestData.Table)).
		Scan(&id); err == nil {
		t.Fatalf("Values were found, but where not expected to be: %v", id)
	}

	var count int
	err = s.(*Session).mysqlSession.
		QueryRow(fmt.Sprintf("SELECT COUNT(id) FROM %s;", writerDeleteTestData.Table)).
		Scan(&count)
	if err != nil {
		t.Errorf("unable to count table, %s", err)
	}
	if count != 9 {
		t.Errorf("wrong document count, expected 9, got %d", count)
	}
}

var (
	writerComplexDeleteTestData = &TestData{"writer_complex_delete_test", "complex_delete_test_table", complexSchema, 10}
)

func TestComplexDelete(t *testing.T) {
	ranInt := rand.Intn(writerComplexDeleteTestData.InsertCount)
	w := newWriter()
	c, err := NewClient(WithURI(fmt.Sprintf("mysql://root@localhost:3306?%s", writerComplexDeleteTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to mysql, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to mysql, %s", err)
	}
	msg := message.From(
		ops.Delete,
		fmt.Sprintf("%s.%s", writerComplexDeleteTestData.DB, writerComplexDeleteTestData.Table),
		data.Data{"id": ranInt+1, "colvar": randomHeros[ranInt]})
	if _, err := w.Write(msg)(s); err != nil {
		t.Errorf("unexpected Delete error, %s\n", err)
	}

	var id int
	if err := s.(*Session).mysqlSession.
		QueryRow(fmt.Sprintf("SELECT id FROM %s WHERE id = %d AND colvar = '%s'", writerComplexDeleteTestData.Table, ranInt+1, randomHeros[ranInt])).
		Scan(&id); err == nil {
		t.Fatalf("Values were found, but where not expected to be: %v", id)
	}
	// Add a row count check as well because if it picks the wrong row due to
	// off-by-one then it'll fail to delete, but _also_ fail to find so will think it's
	// passed
	var count int
	err = s.(*Session).mysqlSession.
		QueryRow(fmt.Sprintf("SELECT COUNT(id) FROM %s;", writerComplexDeleteTestData.Table)).
		Scan(&count)
	if err != nil {
		t.Errorf("unable to count table, %s", err)
	}
	if count != 9 {
		t.Errorf("wrong document count, expected 9, got %d", count)
	}
}

var (
	writerComplexDeletePkTestData = &TestData{"writer_complex_pk_delete_test", "complex_pk_delete_test_table", complexSchema, 10}
)

func TestComplexDeleteWithoutAllPrimarykeys(t *testing.T) {
	// This checks for an expected failure. I.e. should not be possible to delete
	// the row without all primary keys
	ranInt := rand.Intn(writerComplexDeletePkTestData.InsertCount)
	w := newWriter()
	c, err := NewClient(WithURI(fmt.Sprintf("mysql://root@localhost:3306?%s", writerComplexDeletePkTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to mysql, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to mysql, %s", err)
	}
	msg := message.From(
		ops.Delete,
		fmt.Sprintf("%s.%s", writerComplexDeletePkTestData.DB, writerComplexDeletePkTestData.Table),
		data.Data{"id": ranInt+1})
	if _, err := w.Write(msg)(s); err == nil {
		t.Fatalf("Did not receive anticipated error from mysql.writeMessage")
	} else {
		t.Logf("Received expected error: %s", err)
	}

	var id int
	if err := s.(*Session).mysqlSession.
		QueryRow(fmt.Sprintf("SELECT id FROM %s WHERE id = %d AND colvar = '%s'", writerComplexDeletePkTestData.Table,
			ranInt+1,
			randomHeros[ranInt])).
		Scan(&id); err != nil {
		t.Fatalf("Expected to find values, but none were found: %v", err)
	}
	// Add a row count check as well
	var count int
	err = s.(*Session).mysqlSession.
		QueryRow(fmt.Sprintf("SELECT COUNT(id) FROM %s;", writerComplexDeletePkTestData.Table)).
		Scan(&count)
	if err != nil {
		t.Errorf("unable to count table, %s", err)
	}
	if count != 10 {
		t.Errorf("wrong document count, expected 10, got %d", count)
	}
}
