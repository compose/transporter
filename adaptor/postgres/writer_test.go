package postgres

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
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
	w := newWriter()
	c, err := NewClient(WithURI(fmt.Sprintf("postgres://127.0.0.1:5432/%s?sslmode=disable", writerTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to postgres, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to postgres, %s", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := w.Write(
			message.WithConfirms(
				make(chan struct{}),
				message.From(
					ops.Insert,
					fmt.Sprintf("public.%s", writerTestData.Table),
					data.Data{"id": i, "colvar": "hello world", "coltimestamp": time.Now().UTC()}),
			),
		)(s); err != nil {
			t.Errorf("unexpected Insert error, %s\n", err)
		}
	}

	unusedConfirms := make(chan struct{})
	var wg sync.WaitGroup
	var confirmClosed bool
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		select {
		case <-time.After(10 * time.Second):
		case <-unusedConfirms:
			confirmClosed = true
		}
		wg.Done()
	}(&wg)
	if _, err := w.Write(message.WithConfirms(
		unusedConfirms,
		message.From(
			ops.Command,
			fmt.Sprintf("public.%s", writerTestData.Table),
			map[string]interface{}{},
		)),
	)(s); err != nil {
		t.Errorf("unexpected Command error, %s", err)
	}

	wg.Wait()
	if !confirmClosed {
		t.Errorf("confirms chan should have been closed but isn't")
	}

	if _, err := w.Write(message.From(
		ops.Command,
		fmt.Sprintf("public.%s", writerTestData.Table),
		map[string]interface{}{},
	))(s); err != nil {
		t.Errorf("unexpected Command error, %s", err)
	}

	var (
		id          int
		stringValue string
		timeValue   time.Time
	)
	if err := s.(*Session).pqSession.
		QueryRow(fmt.Sprintf("SELECT id, colvar, coltimestamp FROM %s WHERE id = 4", writerTestData.Table)).
		Scan(&id, &stringValue, &timeValue); err != nil {
		t.Fatalf("Error on test query: %v", err)
	}
	if id != 4 || stringValue != "hello world" || timeValue.Before(time.Now().Add(-30*time.Second).UTC()) {
		t.Fatalf("Values were not what they were expected to be: %v, %v, %v", id, stringValue, timeValue)
	}

	var count int
	err = s.(*Session).pqSession.
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

func TestComplexInsert(t *testing.T) {
	w := newWriter()
	c, err := NewClient(WithURI(fmt.Sprintf("postgres://127.0.0.1:5432/%s?sslmode=disable", writerComplexTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to postgres, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to postgres, %s", err)
	}
	for i := 0; i < 10; i++ {
		msg := message.From(ops.Insert, fmt.Sprintf("public.%s", writerComplexTestData.Table), data.Data{
			"id":                 i,
			"colvar":             randomHeros[i],
			"coltimestamp":       time.Now().UTC(),
			"colarrayint":        []interface{}{1, 2, 3, 4},
			"colarraystring":     "{\"one\", \"two\", \"three\", \"four\"}",
			"colbigint":          int64(4000001240125),
			"colbit":             "1",
			"colboolean":         false,
			"colbox":             "(10,10),(20,20)",
			"colbytea":           "\\xDEADBEEF",
			"colcharacter":       "a",
			"colcidr":            "10.0.1.0/28",
			"colcircle":          "<(5,10),3>",
			"coldate":            time.Now().UTC(),
			"coldoubleprecision": 0.314259892323,
			"colenum":            "sad",
			"colinet":            "10.0.1.0",
			"colinteger":         int64(3),
			"coljson":            map[string]interface{}{"name": "batman"},
			"colarrayjson":       []map[string]interface{}{map[string]interface{}{"name": "batman"}, map[string]interface{}{"name": "robin"}},
			"coljsonb":           map[string]interface{}{"name": "batman"},
			"colline":            "{1, 1, 3}",
			"collseg":            "((10,10),(25,25))",
			"colmacaddr":         "08:00:2b:01:02:03",
			"colmoney":           "35.68",
			"colnumeric":         0.23509838,
			"colpath":            "[(10,10),(20,20),(20,10),(15,15)]",
			"colpg_lsn":          "0/3000000",
			"colpoint":           "(15,15)",
			"colpolygon":         "((10,10),(11, 11),(11,0),(5,5))",
			"colreal":            7,
			"colsmallint":        3,
			"coltext":            "this is \n extremely important",
			"coltime":            "13:45",
			"coltsquery":         "'fat':AB & 'cat'",
			"coltsvector":        "a fat cat sat on a mat and ate a fat rat",
			"coluuid":            "f0a0da24-4068-4be4-961d-7c295117ccca",
			"colxml":             "<person><name>Batman</name></person>",
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
	if err := s.(*Session).pqSession.
		QueryRow(fmt.Sprintf("SELECT id, colvar, coltimestamp FROM %s WHERE id = 4", writerComplexTestData.Table)).
		Scan(&id, &stringValue, &timeValue); err != nil {
		t.Fatalf("Error on test query: %v", err)
	}
	if id != 4 || stringValue != randomHeros[4] || timeValue.Before(time.Now().Add(-30*time.Second).UTC()) {
		t.Fatalf("Values were not what they were expected to be: %v, %v, %v", id, stringValue, timeValue)
	}

	var count int
	err = s.(*Session).pqSession.
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
	c, err := NewClient(WithURI(fmt.Sprintf("postgres://127.0.0.1:5432/%s?sslmode=disable", writerUpdateTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to postgres, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to postgres, %s", err)
	}
	for i := 0; i < 10; i++ {
		msg := message.From(
			ops.Insert,
			fmt.Sprintf("public.%s", writerUpdateTestData.Table),
			data.Data{"id": i, "colvar": "hello world", "coltimestamp": time.Now().UTC()})
		if _, err := w.Write(msg)(s); err != nil {
			t.Errorf("unexpected Insert error, %s\n", err)
		}
	}
	msg := message.From(
		ops.Update,
		fmt.Sprintf("public.%s", writerUpdateTestData.Table),
		data.Data{"id": 1, "colvar": "robin", "coltimestamp": time.Now().UTC()})
	if _, err := w.Write(msg)(s); err != nil {
		t.Errorf("unexpected Update error, %s\n", err)
	}

	var (
		id          int
		stringValue string
		timeValue   time.Time
	)
	if err := s.(*Session).pqSession.
		QueryRow(fmt.Sprintf("SELECT id, colvar, coltimestamp FROM %s WHERE id = 1", writerUpdateTestData.Table)).
		Scan(&id, &stringValue, &timeValue); err != nil {
		t.Fatalf("Error on test query: %v", err)
	}
	if id != 1 || stringValue != "robin" || timeValue.Before(time.Now().Add(-30*time.Second).UTC()) {
		t.Fatalf("Values were not what they were expected to be: %v, %v, %v", id, stringValue, timeValue)
	}

	var count int
	err = s.(*Session).pqSession.
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
	c, err := NewClient(WithURI(fmt.Sprintf("postgres://127.0.0.1:5432/%s?sslmode=disable", writerComplexUpdateTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to postgres, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to postgres, %s", err)
	}
	msg := message.From(ops.Update, fmt.Sprintf("public.%s", writerComplexUpdateTestData.Table), data.Data{
		"id":                 ranInt,
		"colvar":             randomHeros[ranInt],
		"colarrayint":        []interface{}{1, 2, 3, 4},
		"colarraystring":     "{\"one\", \"two\", \"three\", \"four\"}",
		"colbigint":          int64(4000001240125),
		"colbit":             "1",
		"colboolean":         false,
		"colbox":             "(10,10),(20,20)",
		"colbytea":           "\\xDEADBEEF",
		"colcharacter":       "a",
		"colcidr":            "10.0.1.0/28",
		"colcircle":          "<(5,10),3>",
		"coldate":            time.Now().UTC(),
		"coldoubleprecision": 0.314259892323,
		"colenum":            "sad",
		"colinet":            "10.0.1.0",
		"colinteger":         int64(3),
		"coljson":            map[string]interface{}{"name": "batman"},
		"colarrayjson":       []map[string]interface{}{map[string]interface{}{"name": "batman"}, map[string]interface{}{"name": "robin"}},
		"coljsonb":           map[string]interface{}{"name": "batman"},
		"colline":            "{1, 1, 3}",
		"collseg":            "((10,10),(25,25))",
		"colmacaddr":         "08:00:2b:01:02:03",
		"colmoney":           "35.68",
		"colnumeric":         0.23509838,
		"colpath":            "[(10,10),(20,20),(20,10),(15,15)]",
		"colpg_lsn":          "0/3000000",
		"colpoint":           "(15,15)",
		"colpolygon":         "((10,10),(11, 11),(11,0),(5,5))",
		"colreal":            7,
		"colsmallint":        3,
		"coltext":            "this is \n extremely important",
		"coltime":            "13:45",
		"coltsquery":         "'fat':AB & 'cat'",
		"coltsvector":        "a fat cat sat on a mat and ate a fat rat",
		"coluuid":            "f0a0da24-4068-4be4-961d-7c295117ccca",
		"colxml":             "<person><name>Batman</name></person>",
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
	if err := s.(*Session).pqSession.
		QueryRow(fmt.Sprintf("SELECT id, colvar, coltimestamp, colbigint FROM %s WHERE id = %d", writerComplexUpdateTestData.Table, ranInt)).
		Scan(&id, &stringValue, &timeValue, &bigint); err != nil {
		t.Fatalf("Error on test query: %v", err)
	}
	if id != ranInt || stringValue != randomHeros[ranInt] || timeValue.Before(time.Now().Add(-30*time.Second).UTC()) || bigint != int64(4000001240125) {
		t.Fatalf("Values were not what they were expected to be: %v, %v, %v, %v", id, stringValue, timeValue, bigint)
	}

	var count int
	err = s.(*Session).pqSession.
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
	c, err := NewClient(WithURI(fmt.Sprintf("postgres://127.0.0.1:5432/%s?sslmode=disable", writerDeleteTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to postgres, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to postgres, %s", err)
	}
	for i := 0; i < 10; i++ {
		msg := message.From(
			ops.Insert,
			fmt.Sprintf("public.%s", writerDeleteTestData.Table),
			data.Data{"id": i, "colvar": "hello world", "coltimestamp": time.Now().UTC()})
		if _, err := w.Write(msg)(s); err != nil {
			t.Errorf("unexpected Insert error, %s\n", err)
		}
	}
	msg := message.From(ops.Delete, fmt.Sprintf("public.%s", writerDeleteTestData.Table), data.Data{"id": 1})
	if _, err := w.Write(msg)(s); err != nil {
		t.Errorf("unexpected Update error, %s\n", err)
	}

	var id int
	if err := s.(*Session).pqSession.
		QueryRow(fmt.Sprintf("SELECT id FROM %s WHERE id = 1", writerDeleteTestData.Table)).
		Scan(&id); err == nil {
		t.Fatalf("Values were found, but where not expected to be: %v", id)
	}

	var count int
	err = s.(*Session).pqSession.
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
	c, err := NewClient(WithURI(fmt.Sprintf("postgres://127.0.0.1:5432/%s?sslmode=disable", writerComplexDeleteTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to postgres, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to postgres, %s", err)
	}
	msg := message.From(
		ops.Delete,
		fmt.Sprintf("public.%s", writerComplexDeleteTestData.Table),
		data.Data{"id": ranInt, "colvar": randomHeros[ranInt]})
	if _, err := w.Write(msg)(s); err != nil {
		t.Errorf("unexpected Delete error, %s\n", err)
	}

	var id int
	if err := s.(*Session).pqSession.
		QueryRow(fmt.Sprintf("SELECT id FROM %s WHERE id = %d AND colvar = '%s'", writerComplexDeleteTestData.Table, ranInt, randomHeros[ranInt])).
		Scan(&id); err == nil {
		t.Fatalf("Values were found, but where not expected to be: %v", id)
	}
}

var (
	writerComplexDeletePkTestData = &TestData{"writer_complex_pk_delete_test", "complex_pk_delete_test_table", complexSchema, 10}
)

func TestComplexDeleteWithoutAllPrimarykeys(t *testing.T) {
	ranInt := rand.Intn(writerComplexDeletePkTestData.InsertCount)
	w := newWriter()
	c, err := NewClient(WithURI(fmt.Sprintf("postgres://127.0.0.1:5432/%s?sslmode=disable", writerComplexDeletePkTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to postgres, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to postgres, %s", err)
	}
	msg := message.From(ops.Delete, fmt.Sprintf("public.%s", writerComplexDeletePkTestData.Table), data.Data{"id": ranInt})
	if _, err := w.Write(msg)(s); err == nil {
		t.Fatalf("Did not receive anticipated error from postgres.writeMessage")
	}

	var id int
	if err := s.(*Session).pqSession.
		QueryRow(fmt.Sprintf("SELECT id FROM %s WHERE id = %d AND colvar = '%s'", writerComplexDeletePkTestData.Table,
			ranInt,
			randomHeros[ranInt])).
		Scan(&id); err != nil {
		t.Fatalf("Expected to find values, but none were found: %v", err)
	}
}
