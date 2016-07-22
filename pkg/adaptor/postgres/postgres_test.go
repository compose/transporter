package adaptor

import (
	//"fmt"

	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"git.compose.io/compose/transporter/pkg/adaptor"
	"git.compose.io/compose/transporter/pkg/message"
	"git.compose.io/compose/transporter/pkg/message/data"
	"git.compose.io/compose/transporter/pkg/message/ops"
	"git.compose.io/compose/transporter/pkg/pipe"
	_ "github.com/lib/pq"
)

func TestInsert(t *testing.T) {
	p, err := createSimpleTestTable()
	if err != nil {
		t.Fatalf("Error initializing data set: %v", err)
	}
	defer func() {
		p.session.Exec("DROP TABLE IF EXISTS simple_test_table;")
		p.session.Close()
	}()
	msg := message.MustUseAdaptor("postgres").From(ops.Insert, "public.simple_test_table", data.Data{"id": 4, "colvar": "string", "coltimestamp": time.Now().UTC()})
	if _, err = p.writeMessage(msg); err != nil {
		t.Fatalf("Error pushing insert to Postgres: %v", err)
	}

	var (
		id          int
		stringValue string
		timeValue   time.Time
	)
	err = p.session.QueryRow("SELECT id, colvar, coltimestamp FROM simple_test_table WHERE id = 4").Scan(&id, &stringValue, &timeValue)
	if err != nil {
		t.Fatalf("Error on test query: %v", err)
	}
	if id != 4 || stringValue != "string" || timeValue.Before(time.Now().Add(-30*time.Second).UTC()) {
		t.Fatalf("Values were not what they were expected to be: %v, %v, %v", id, stringValue, timeValue)
	}
}

func TestUpdate(t *testing.T) {
	p, err := createSimpleTestTable()
	if err != nil {
		t.Fatalf("Error initializing data set: %v", err)
	}
	defer func() {
		p.session.Exec("DROP TABLE IF EXISTS simple_test_table;")
		p.session.Close()
	}()
	msg := message.MustUseAdaptor("postgres").From(ops.Update, "public.simple_test_table", data.Data{"id": 1, "colvar": "robin", "coltimestamp": time.Now().UTC()})
	if _, err = p.writeMessage(msg); err != nil {
		t.Fatalf("Error pushing update to Postgres: %v", err)
	}

	var (
		id          int
		stringValue string
		timeValue   time.Time
	)
	err = p.session.QueryRow("SELECT id, colvar, coltimestamp FROM simple_test_table WHERE id = 1").Scan(&id, &stringValue, &timeValue)
	if err != nil {
		t.Fatalf("Error on test query: %v", err)
	}
	if id != 1 || stringValue != "robin" || timeValue.Before(time.Now().Add(-30*time.Second).UTC()) {
		t.Fatalf("Values were not what they were expected to be: %v, %v, %v", id, stringValue, timeValue)
	}

}

func TestComplexUpdate(t *testing.T) {
	p, err := createComplexTestTable()
	if err != nil {
		t.Fatalf("Error initializing data set: %v", err)
	}
	defer func() {
		p.session.Exec("DROP TABLE complex_test_table;")
		p.session.Close()
	}()
	msg := message.MustUseAdaptor("postgres").From(ops.Update, "public.complex_test_table", data.Data{
		"id":                 1,
		"colvar":             "Wonder Woman",
		"colarrayint":        []interface{}{1, 2, 3, 4},
		"colarraystring":     "{\"o,ne\", \"two\", \"three\", \"four\"}",
		"colbigint":          int64(4000001240124),
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
	if _, err = p.writeMessage(msg); err != nil {
		t.Fatalf("Error pushing update to Postgres: %v", err)
	}

	var (
		id          int
		stringValue string
		timeValue   time.Time
	)
	err = p.session.QueryRow("SELECT id, colvar, coltimestamp FROM complex_test_table WHERE id = 1").Scan(&id, &stringValue, &timeValue)
	if err != nil {
		t.Fatalf("Error on test query: %v", err)
	}
	if id != 1 || stringValue != "Wonder Woman" || timeValue.Before(time.Now().Add(-30*time.Second).UTC()) {
		t.Fatalf("Values were not what they were expected to be: %v, %v, %v", id, stringValue, timeValue)
	}
}

func TestDelete(t *testing.T) {
	p, err := createSimpleTestTable()
	if err != nil {
		t.Fatalf("Error initializing data set: %v", err)
	}
	defer func() {
		p.session.Exec("DROP TABLE simple_test_table;")
		p.session.Close()
	}()
	msg := message.MustUseAdaptor("postgres").From(ops.Delete, "public.simple_test_table", data.Data{"id": 1})
	if _, err = p.writeMessage(msg); err != nil {
		t.Fatalf("Error pushing update to Postgres: %v", err)
	}

	var (
		id int
	)
	err = p.session.QueryRow("SELECT id FROM simple_test_table WHERE id = 1").Scan(&id)
	if err == nil {
		t.Fatalf("Values were found, but where not expected to be: %v", id)
	}
}

func TestComplexDelete(t *testing.T) {
	p, err := createComplexTestTable()
	if err != nil {
		t.Fatalf("Error initializing data set: %v", err)
	}
	defer func() {
		p.session.Exec("DROP TABLE complex_test_table;")
		p.session.Close()
	}()
	msg := message.MustUseAdaptor("postgres").From(ops.Delete, "public.complex_test_table", data.Data{"id": 1, "colvar": "Wonder Woman"})
	if _, err = p.writeMessage(msg); err != nil {
		t.Fatalf("Error pushing update to Postgres: %v", err)
	}

	var (
		id int
	)
	err = p.session.QueryRow("SELECT id FROM complex_test_table WHERE id = 1 AND colvar = 'Wonder Woman'").Scan(&id)
	if err == nil {
		t.Fatalf("Values were found, but where not expected to be: %v", id)
	}
}

func TestComplexDeleteWithoutAllPrimarykeys(t *testing.T) {
	p, err := createComplexTestTable()
	defer p.session.Close()
	if err != nil {
		t.Fatalf("Error initializing data set: %v", err)
	}
	defer func() {
		p.session.Exec("DROP TABLE complex_test_table;")
	}()
	msg := message.MustUseAdaptor("postgres").From(ops.Delete, "public.complex_test_table", data.Data{"id": 1})
	if _, err := p.writeMessage(msg); err == nil {
		t.Fatalf("Did not receive anticipated error from writeMessage")
	}

	var (
		id int
	)
	err = p.session.QueryRow("SELECT id FROM complex_test_table WHERE id = 1 AND colvar = 'Wonder Woman'").Scan(&id)
	if err != nil {
		t.Fatalf("Expected to find values, but none were found: %v", err)
	}
}

// test the insert / update / delete against a simple table
func TestSimpleTail(t *testing.T) {
	p, err := createSimpleTestTable()
	if err != nil {
		t.Fatalf("Error creating simple test table: %v", err)
	}
	defer func() {
		p.session.Exec("SELECT * FROM pg_drop_replication_slot('test_slot');")
		p.session.Exec("DROP TABLE simple_test_table;")
		p.session.Close()
	}()
	if err = addTestReplicationSlot(p); err != nil {
		t.Fatalf("Error creating replication slot: %v", err)
	}

	// Test insert
	_, err = p.session.Exec("INSERT INTO simple_test_table (colvar, coltimestamp) VALUES ('Superman', now() at time zone 'utc');")
	if err != nil {
		t.Fatalf("Error inserting seed data: %v", err)
	}
	time.Sleep(1 * time.Second) // give time for Postgres to create logical decoding records
	msgSlice, err := p.pluckFromLogicalDecoding()
	if err != nil {
		t.Fatalf("Error plucking latest changes: %v", err)
	}

	msg := msgSlice[0]
	if msg.OP() != ops.Insert {
		t.Fatalf("Expected action to be an insert action, but was %v", msg.OP())
	}
	if !reflect.DeepEqual(msg.Data().Get("id"), 2) {
		t.Fatalf("Expected message to be 2, but was %v", msg.Data().Get("id"))
	}
	if !reflect.DeepEqual(msg.Data().Get("colvar"), "Superman") {
		t.Fatalf("Expected colvar to be 'Superman', but was %v", msg.Data().Get("colvar"))
	}
	if msg.Namespace() != "public.simple_test_table" {
		t.Fatalf("Expected namespace to be public.simple_test_table, but was %v", msg.Namespace())
	}

	// Test update
	_, err = p.session.Exec("UPDATE simple_test_table SET colvar = 'Robin' WHERE id = 2;")
	if err != nil {
		t.Fatalf("Error inserting seed data: %v", err)
	}
	time.Sleep(1 * time.Second) // give time for Postgres to create logical decoding records
	msgSlice, err = p.pluckFromLogicalDecoding()
	if err != nil {
		t.Fatalf("Error plucking latest changes: %v", err)
	}

	msg = msgSlice[0]
	if msg.OP() != ops.Update {
		t.Fatalf("Expected action to be an insert action, but was %v", msg.OP())
	}
	if msg.Data().Get("id") != 2 || msg.Data().Get("colvar") != "Robin" {
		t.Fatalf("Expected message to be {id: 2, colvar: 'Robin'}, but was %v", msg.Data())
	}
	if msg.Namespace() != "public.simple_test_table" {
		t.Fatalf("Expected namespace to be public.simple_test_table, but was %v", msg.Namespace())
	}

	// Test delete
	_, err = p.session.Exec("DELETE FROM simple_test_table WHERE id = 2;")
	if err != nil {
		t.Fatalf("Error inserting seed data: %v", err)
	}
	time.Sleep(1 * time.Second) // give time for Postgres to create logical decoding records
	msgSlice, err = p.pluckFromLogicalDecoding()
	if err != nil {
		t.Fatalf("Error plucking latest changes: %v", err)
	}

	msg = msgSlice[0]
	if msg.OP() != ops.Delete {
		t.Fatalf("Expected action to be an insert action, but was %v", msg.OP())
	}
	if msg.Data().Get("id") != 2 {
		t.Fatalf("Expected message to be {id: 2}, but was %v", msg.Data())
	}
	if msg.Namespace() != "public.simple_test_table" {
		t.Fatalf("Expected namespace to be public.simple_test_table, but was %v", msg.Namespace())
	}
}

// test the insert / update / delete against a complex table
func TestComplexTail(t *testing.T) {
	p, err := createComplexTestTable()
	if err != nil {
		t.Fatalf("Error creating complex test table: %v", err)
	}
	defer func() {
		p.session.Exec("SELECT * FROM pg_drop_replication_slot('test_slot');")
		p.session.Exec("DROP TABLE complex_test_table;")
		p.session.Close()
	}()
	if err = addTestReplicationSlot(p); err != nil {
		t.Fatalf("Error creating replication slot: %v", err)
	}

	// Test insert
	_, err = p.session.Exec(complexTableInsert)
	if err != nil {
		t.Fatalf("Error inserting seed data: %v", err)
	}
	time.Sleep(1 * time.Second) // give time for Postgres to create logical decoding records
	msgSlice, err := p.pluckFromLogicalDecoding()
	if err != nil {
		t.Fatalf("Error plucking latest changes: %v", err)
	}

	msg := msgSlice[0]
	if msg.OP() != ops.Insert {
		t.Fatalf("Expected action to be an insert action, but was %v", msg.OP())
	}

	// test base values
	expectedValues := map[string]interface{}{
		"colbigint":      4000001240124,
		"colboolean":     false,
		"coldate":        time.Date(time.Now().UTC().Year(), time.Now().UTC().Month(), time.Now().UTC().Day(), 0, 0, 0, 0, time.UTC),
		"colpg_lsn":      "0/3000000",
		"colmoney":       35.68,
		"colnumeric":     0.23509838,
		"colsmallint":    3,
		"colsmallserial": 2,
		"colenum":        "sad",
	}
	for key, expectedValue := range expectedValues {
		if !reflect.DeepEqual(msg.Data().Get(key), expectedValue) {
			t.Fatalf("Expected msg['%v'] to = %v, but was %v", key, expectedValue, msg.Data().Get(key))
		}
	}

	// test array values
	expectedArrayValues := map[string][]interface{}{
		"colarrayint":    []interface{}{1, 2, 3, 4},
		"colarraystring": []interface{}{"o,ne", "two", "three", "four"},
	}
	for key, expectedArray := range expectedArrayValues {
		for i, expectedValue := range expectedArray {
			if msg.Data().Get(key).([]interface{})[i] != expectedValue {
				t.Fatalf("Expected msg['%v'][%v] to = %v, but was %v", key, i, expectedValue, msg.Data().Get(key).([]interface{})[i])
			}
		}
	}

	// test map values
	//"coljson":        map[string]interface{}{"name": "batman"},
	//"coljsonb":       map[string]interface{}{"name": "alfred"},
	if msg.Data().Get("coljson").(map[string]interface{})["name"] != "batman" {
		t.Fatalf("Expected msg['%v']['name'] to = %v, but was %v", "coljson", "batman", msg.Data().Get("coljson").(map[string]interface{})["name"])
	}

	if msg.Namespace() != "public.complex_test_table" {
		t.Fatalf("Expected namespace to be public.complex_test_table, but was %v", msg.Namespace())
	}
}

func TestCatSimpleTable(t *testing.T) {
	// setup
	p, err := createSimpleTestTable()
	if err != nil {
		t.Fatalf("Error initializing data set: %v", err)
	}
	defer func() {
		p.session.Exec("DROP TABLE simple_test_table;")
		p.session.Close()
	}()

	// run
	result, err := p.catTable("public", "simple_test_table", nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	// Test values in result map
	for key, value := range map[string]interface{}{
		"colvar": "batman",
		"id":     int64(1),
	} {
		if result[0].Data().Get(key) != value {
			t.Fatalf("Expected %v of row to equal %v (%T), but was %v (%T)", key, value, value, result[0].Data().Get(key), result[0].Data().Get(key))
		}
	}
}

func TestCatComplexTable(t *testing.T) {
	// setup
	p, err := createComplexTestTable()
	if err != nil {
		t.Fatalf("Error initializing data set: %v", err)
	}
	defer func() {
		p.session.Exec("DROP TABLE complex_test_table;")
		p.session.Close()
	}()

	// run
	result, err := p.catTable("public", "complex_test_table", nil)

	if err != nil {
		t.Fatalf(err.Error())
	}

	// Test values in result map
	for key, value := range map[string]interface{}{
		"id":                 int64(1),
		"colvar":             "Wonder Woman",
		"colbigint":          int64(4000001240124),
		"colbit":             "1",
		"colboolean":         false,
		"colbox":             "(20,20),(10,10)",
		"colcharacter":       "a",
		"colcidr":            "10.0.1.0/28",
		"colcircle":          "<(5,10),3>",
		"coldoubleprecision": 0.314259892323,
		"colenum":            "sad",
		"colinet":            "10.0.1.0",
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
		if result[0].Data().Get(key) != value {
			t.Fatalf("Expected %v of row to equal %v (%T), but was %v (%T)", key, value, value, result[0].Data().Get(key), result[0].Data().Get(key))
		}
	}
}

func createDatabaseIfNecessary() error {
	pg, err := sql.Open("postgres", "host=localhost sslmode=disable")
	if err != nil {
		return err
	}
	pg.Exec(`DROP DATABASE IF EXISTS transporter_test`)
	_, err = pg.Exec(`
		CREATE DATABASE transporter_test;
	`)
	if err != nil {
		return err
	}
	return pg.Close()
}

func NewTestPostgres() (postgres *Postgres, err error) {
	// Create the database
	err = createDatabaseIfNecessary()
	if err != nil {
		return nil, err
	}
	config := map[string]interface{}{
		"type":             "postgres",
		"uri":              "host=localhost sslmode=disable dbname=transporter_test",
		"namespace":        "transporter_test.public..*",
		"replication_slot": "test_slot",
	}

	ppipe := pipe.NewPipe(nil, "some name")
	go func() {
		for {
			<-ppipe.Err
			// noop ignore piped errors
		}
	}()
	a, err := adaptor.CreateAdaptor("postgres", "", config, ppipe)
	if err != nil {
		return postgres, err
	}

	p, ok := a.(*Postgres)
	if !ok {
		return p, fmt.Errorf("could not create postgres adaptor")
	}
	return p, err
}

func createSimpleTestTable() (*Postgres, error) {
	p, err := NewTestPostgres()
	if err != nil {
		return p, err
	}
	_, err = p.session.Exec(`
		DROP TABLE IF EXISTS simple_test_table;
  `)
	if err != nil {
		return p, err
	}
	_, err = p.session.Exec(`CREATE TABLE simple_test_table (
	      id SERIAL PRIMARY KEY,
	      colvar VARCHAR(255),
	      coltimestamp TIMESTAMP
	    );`)
	if err != nil {
		return p, err
	}
	_, err = p.session.Exec(`
    INSERT INTO simple_test_table (colvar, coltimestamp) VALUES ('batman', now() at time zone 'utc');
  `)

	return p, err
}

func addTestReplicationSlot(p *Postgres) error {
	_, err := p.session.Exec(`
    SELECT * FROM pg_create_logical_replication_slot('test_slot', 'test_decoding');
  `)
	return err
}

const complexTableInsert = `
     INSERT INTO complex_test_table VALUES (
        DEFAULT,                  -- id
        'Wonder Woman',           -- colvar VARCHAR(255),
        now() at time zone 'utc', -- coltimestamp TIMESTAMP,

        '{1, 2, 3, 4}',           -- colarrayint ARRAY[4],
        '{"o,ne", "two", "three", "four"}' , -- colarraystring ARRAY[4],
        4000001240124,       -- colbigint bigint,
        DEFAULT,             -- colbigserial bigserial,
        B'1',                -- colbit bit,
        false,               -- colboolean boolean,
        '(10,10),(20,20)',   -- colbox box,
        E'\\xDEADBEEF',      -- colbytea bytea,
        'a',                 -- colcharacter character(1),
        '10.0.1.0/28',       -- colcidr cidr,
        '<(5, 10), 3>',      -- colcircle circle,
        now() at time zone 'utc', -- coldate date,
        0.314259892323,      -- coldoubleprecision double precision,
        'sad',               -- colenum mood,
        '10.0.1.0',          -- colinet inet,
        3,                   -- colinteger integer,
        DEFAULT,             -- autoset colinterval interval,
        '{"name": "batman"}',  -- coljson json,
        '{"name": "alfred"}',  -- coljsonb jsonb,
        '{1, 1, 3}',         -- colline line,
        '[(10,10),(25,25)]', -- collseg lseg,
        '08:00:2b:01:02:03', -- colmacaddr macaddr,
        35.68,               -- colmoney money,
        0.23509838,   -- colnumeric numeric(8,8),
        '[(10,10),(20,20),(20,10),(15,15)]', -- colpath path,
        '0/3000000',         -- colpg_lsn pg_lsn,
        '(15,15)',           -- colpoint point,
        '((10,10),(11, 11),(11,0),(5,5))', -- colpolygon polygon,
        7,                   -- colreal real,
        DEFAULT,             -- colserial serial,
        3,                   -- colsmallint smallint,
        DEFAULT,             -- colsmallserial smallserial,
        'this is \n extremely important', -- coltext text,
        '13:45',             -- coltime time,
        'fat:ab & cat',      -- coltsquery tsquery,
        'a fat cat sat on a mat and ate a fat rat', -- coltsvector tsvector,
        null,
        'f0a0da24-4068-4be4-961d-7c295117ccca', -- coluuid uuid,
        '<person><name>Batman</name></person>' --    colxml xml,
      );
`

func createComplexTestTable() (*Postgres, error) {
	p, err := NewTestPostgres()
	if err != nil {
		return p, err
	}

	p.session.Exec("CREATE TYPE mood AS ENUM('sad', 'ok', 'happy');")
	_, err = p.session.Exec(`
		DROP TABLE IF EXISTS complex_test_table;
	`)
	if err != nil {
		return p, err
	}
	_, err = p.session.Exec(`
    CREATE TABLE complex_test_table (
      id SERIAL,

      colvar VARCHAR(255),
      coltimestamp TIMESTAMP,

      colarrayint integer ARRAY[4],
      colarraystring varchar ARRAY[4],
      colbigint bigint,
      colbigserial bigserial,
      colbit bit,
      colboolean boolean,
      colbox box,
      colbytea bytea,
      colcharacter character,
      colcidr cidr,
      colcircle circle,
      coldate date,
      coldoubleprecision double precision,
      colenum mood,
      colinet inet,
      colinteger integer,
      colinterval interval,
      coljson json,
      coljsonb jsonb,
      colline line,
      collseg lseg,
      colmacaddr macaddr,
      colmoney money,
      colnumeric numeric(8,8),
      colpath path,
      colpg_lsn pg_lsn,
      colpoint point,
      colpolygon polygon,
      colreal real,
      colserial serial,
      colsmallint smallint,
      colsmallserial smallserial,
      coltext text,
      coltime time,
      coltsquery tsquery,
      coltsvector tsvector,
      coltxid_snapshot txid_snapshot,
      coluuid uuid,
      colxml xml,

      PRIMARY KEY (id, colvar)
    );
  `)
	if err != nil {
		return p, err
	}

	_, err = p.session.Exec(complexTableInsert)
	return p, err
}
