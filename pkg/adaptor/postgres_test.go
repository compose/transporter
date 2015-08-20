package adaptor

import (
	//"fmt"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
)

func TestInsert(t *testing.T) {
	postgres, err := createSimpleTestTable()
	if err != nil {
		t.Fatalf("Error initializing data set: %v", err)
	}
	defer func() {
		postgres.postgresSession.Exec("DROP TABLE simple_test_table;")
	}()

	msg := message.NewMsg(message.Insert, map[string]interface{}{"id": 4, "colvar": "string", "coltimestamp": time.Now().UTC()}, "public.simple_test_table")
	if _, err := postgres.writeMessage(msg); err != nil {
		t.Fatalf("Error pushing insert to Postgres: %v", err)
	}

	var (
		id          int
		stringValue string
		timeValue   time.Time
	)
	err = postgres.postgresSession.QueryRow("SELECT id, colvar, coltimestamp FROM simple_test_table WHERE id = 4").Scan(&id, &stringValue, &timeValue)
	if err != nil {
		t.Fatalf("Error on test query: %v", err)
	}
	if id != 4 || stringValue != "string" || timeValue.Before(time.Now().Add(-30*time.Second).UTC()) {
		t.Fatalf("Values were not what they were expected to be: %v, %v, %v", id, stringValue, timeValue)
	}
}

func TestUpdate(t *testing.T) {
	postgres, err := createSimpleTestTable()
	if err != nil {
		t.Fatalf("Error initializing data set: %v", err)
	}
	defer func() {
		postgres.postgresSession.Exec("DROP TABLE simple_test_table;")
	}()

	msg := message.NewMsg(message.Update, map[string]interface{}{"id": 1, "colvar": "robin", "coltimestamp": time.Now().UTC()}, "public.simple_test_table")
	if _, err := postgres.writeMessage(msg); err != nil {
		t.Fatalf("Error pushing update to Postgres: %v", err)
	}

	var (
		id          int
		stringValue string
		timeValue   time.Time
	)
	err = postgres.postgresSession.QueryRow("SELECT id, colvar, coltimestamp FROM simple_test_table WHERE id = 1").Scan(&id, &stringValue, &timeValue)
	if err != nil {
		t.Fatalf("Error on test query: %v", err)
	}
	if id != 1 || stringValue != "robin" || timeValue.Before(time.Now().Add(-30*time.Second).UTC()) {
		t.Fatalf("Values were not what they were expected to be: %v, %v, %v", id, stringValue, timeValue)
	}

}

func TestComplexUpdate(t *testing.T) {
	postgres, err := createComplexTestTable()
	if err != nil {
		t.Fatalf("Error initializing data set: %v", err)
	}
	defer func() {
		postgres.postgresSession.Exec("DROP TABLE complex_test_table;")
	}()

	msg := message.NewMsg(message.Update, map[string]interface{}{
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
	}, "public.complex_test_table")
	if _, err := postgres.writeMessage(msg); err != nil {
		t.Fatalf("Error pushing update to Postgres: %v", err)
	}

	var (
		id          int
		stringValue string
		timeValue   time.Time
	)
	err = postgres.postgresSession.QueryRow("SELECT id, colvar, coltimestamp FROM complex_test_table WHERE id = 1").Scan(&id, &stringValue, &timeValue)
	if err != nil {
		t.Fatalf("Error on test query: %v", err)
	}
	if id != 1 || stringValue != "Wonder Woman" || timeValue.Before(time.Now().Add(-30*time.Second).UTC()) {
		t.Fatalf("Values were not what they were expected to be: %v, %v, %v", id, stringValue, timeValue)
	}
}

func TestDelete(t *testing.T) {
	postgres, err := createSimpleTestTable()
	if err != nil {
		t.Fatalf("Error initializing data set: %v", err)
	}
	defer func() {
		postgres.postgresSession.Exec("DROP TABLE simple_test_table;")
	}()

	msg := message.NewMsg(message.Delete, map[string]interface{}{"id": 1}, "public.simple_test_table")
	if _, err := postgres.writeMessage(msg); err != nil {
		t.Fatalf("Error pushing update to Postgres: %v", err)
	}

	var (
		id int
	)
	err = postgres.postgresSession.QueryRow("SELECT id FROM simple_test_table WHERE id = 1").Scan(&id)
	if err == nil {
		t.Fatalf("Values were found, but where not expected to be: %v", id)
	}
}

func TestComplexDelete(t *testing.T) {
	postgres, err := createComplexTestTable()
	if err != nil {
		t.Fatalf("Error initializing data set: %v", err)
	}
	defer func() {
		postgres.postgresSession.Exec("DROP TABLE complex_test_table;")
	}()

	msg := message.NewMsg(message.Delete, map[string]interface{}{"id": 1, "colvar": "Wonder Woman"}, "public.complex_test_table")
	if _, err := postgres.writeMessage(msg); err != nil {
		t.Fatalf("Error pushing update to Postgres: %v", err)
	}

	var (
		id int
	)
	err = postgres.postgresSession.QueryRow("SELECT id FROM complex_test_table WHERE id = 1 AND colvar = 'Wonder Woman'").Scan(&id)
	if err == nil {
		t.Fatalf("Values were found, but where not expected to be: %v", id)
	}
}

func TestComplexDeleteWithoutAllPrimarykeys(t *testing.T) {
	postgres, err := createComplexTestTable()
	if err != nil {
		t.Fatalf("Error initializing data set: %v", err)
	}
	defer func() {
		postgres.postgresSession.Exec("DROP TABLE complex_test_table;")
	}()

	msg := message.NewMsg(message.Delete, map[string]interface{}{"id": 1}, "public.complex_test_table")
	if _, err := postgres.writeMessage(msg); err == nil {
		t.Fatalf("Did not receive anticipated error from writeMessage")
	}

	var (
		id int
	)
	err = postgres.postgresSession.QueryRow("SELECT id FROM complex_test_table WHERE id = 1 AND colvar = 'Wonder Woman'").Scan(&id)
	if err != nil {
		t.Fatalf("Expected to find values, but none were found: %v", err)
	}
}

// test the insert / update / delete against a simple table
func TestSimpleTail(t *testing.T) {
	postgres, err := createSimpleTestTable()
	if err != nil {
		t.Fatalf("Error creating simple test table: %v", err)
	}
	defer func() {
		postgres.postgresSession.Exec("SELECT * FROM pg_drop_replication_slot('test_slot');")
		postgres.postgresSession.Exec("DROP TABLE simple_test_table;")
	}()
	if err = addTestReplicationSlot(postgres); err != nil {
		t.Fatalf("Error creating replication slot: %v", err)
	}

	// Test insert
	_, err = postgres.postgresSession.Exec("INSERT INTO simple_test_table (colvar, coltimestamp) VALUES ('Superman', now());")
	if err != nil {
		t.Fatalf("Error inserting seed data: %v", err)
	}
	msgSlice, err := postgres.pluckFromLogicalDecoding()
	if err != nil {
		t.Fatalf("Error plucking latest changes: %v", err)
	}

	msg := msgSlice[0]
	if msg.Op != message.Insert {
		t.Fatalf("Expected action to be an insert action, but was %v", msg.Op)
	}
	if msg.Map()["id"] != 2 || msg.Map()["colvar"] != "Superman" {
		t.Fatalf("Expected message to be {id: 2, colvar: 'Superman'}, but was %v", msg.Map())
	}
	if msg.Namespace != "public.simple_test_table" {
		t.Fatalf("Expected namespace to be public.simple_test_table, but was %v", msg.Namespace)
	}

	// Test update
	_, err = postgres.postgresSession.Exec("UPDATE simple_test_table SET colvar = 'Robin' WHERE id = 2;")
	if err != nil {
		t.Fatalf("Error inserting seed data: %v", err)
	}
	msgSlice, err = postgres.pluckFromLogicalDecoding()
	if err != nil {
		t.Fatalf("Error plucking latest changes: %v", err)
	}

	msg = msgSlice[0]
	if msg.Op != message.Update {
		t.Fatalf("Expected action to be an insert action, but was %v", msg.Op)
	}
	if msg.Map()["id"] != 2 || msg.Map()["colvar"] != "Robin" {
		t.Fatalf("Expected message to be {id: 2, colvar: 'Robin'}, but was %v", msg.Map())
	}
	if msg.Namespace != "public.simple_test_table" {
		t.Fatalf("Expected namespace to be public.simple_test_table, but was %v", msg.Namespace)
	}

	// Test delete
	_, err = postgres.postgresSession.Exec("DELETE FROM simple_test_table WHERE id = 2;")
	if err != nil {
		t.Fatalf("Error inserting seed data: %v", err)
	}
	msgSlice, err = postgres.pluckFromLogicalDecoding()
	if err != nil {
		t.Fatalf("Error plucking latest changes: %v", err)
	}

	msg = msgSlice[0]
	if msg.Op != message.Delete {
		t.Fatalf("Expected action to be an insert action, but was %v", msg.Op)
	}
	if msg.Map()["id"] != 2 {
		t.Fatalf("Expected message to be {id: 2}, but was %v", msg.Map())
	}
	if msg.Namespace != "public.simple_test_table" {
		t.Fatalf("Expected namespace to be public.simple_test_table, but was %v", msg.Namespace)
	}
}

// test the insert / update / delete against a complex table
func TestComplexTail(t *testing.T) {
	postgres, err := createComplexTestTable()
	if err != nil {
		t.Fatalf("Error creating complex test table: %v", err)
	}
	defer func() {
		postgres.postgresSession.Exec("SELECT * FROM pg_drop_replication_slot('test_slot');")
		postgres.postgresSession.Exec("DROP TABLE complex_test_table;")
	}()
	if err = addTestReplicationSlot(postgres); err != nil {
		t.Fatalf("Error creating replication slot: %v", err)
	}

	// Test insert
	_, err = postgres.postgresSession.Exec(complexTableInsert)
	if err != nil {
		t.Fatalf("Error inserting seed data: %v", err)
	}
	msgSlice, err := postgres.pluckFromLogicalDecoding()
	if err != nil {
		t.Fatalf("Error plucking latest changes: %v", err)
	}

	msg := msgSlice[0]
	if msg.Op != message.Insert {
		t.Fatalf("Expected action to be an insert action, but was %v", msg.Op)
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
		if msg.Map()[key] != expectedValue {
			t.Fatalf("Expected msg['%v'] to = %v, but was %v", key, expectedValue, msg.Map()[key])
		}
	}

	// test array values
	expectedArrayValues := map[string][]interface{}{
		"colarrayint":    []interface{}{1, 2, 3, 4},
		"colarraystring": []interface{}{"o,ne", "two", "three", "four"},
	}
	for key, expectedArray := range expectedArrayValues {
		for i, expectedValue := range expectedArray {
			if msg.Map()[key].([]interface{})[i] != expectedValue {
				t.Fatalf("Expected msg['%v'][%v] to = %v, but was %v", key, i, expectedValue, msg.Map()[key].([]interface{})[i])
			}
		}
	}

	// test map values
	//"coljson":        map[string]interface{}{"name": "batman"},
	//"coljsonb":       map[string]interface{}{"name": "alfred"},
	if msg.Map()["coljson"].(map[string]interface{})["name"] != "batman" {
		t.Fatalf("Expected msg['%v']['name'] to = %v, but was %v", "coljson", "batman", msg.Map()["coljson"].(map[string]interface{})["name"])
	}

	if msg.Namespace != "public.complex_test_table" {
		t.Fatalf("Expected namespace to be public.complex_test_table, but was %v", msg.Namespace)
	}
}

func TestCatSimpleTable(t *testing.T) {
	// setup
	postgres, err := createSimpleTestTable()
	if err != nil {
		t.Fatalf("Error initializing data set: %v", err)
	}
	defer func() {
		postgres.postgresSession.Exec("DROP TABLE simple_test_table;")
	}()

	// run
	result, err := postgres.catTable("public", "simple_test_table", nil)

	// Test values in result map
	for key, value := range map[string]interface{}{
		"colvar": "batman",
		"id":     int64(1),
	} {
		if result[0].Map()[key] != value {
			t.Fatalf("Expected %v of row to equal %v (%T), but was %v (%T)", key, value, value, result[0].Map()[key], result[0].Map()[key])
		}
	}
}

func TestCatComplexTable(t *testing.T) {
	// setup
	postgres, err := createComplexTestTable()
	if err != nil {
		t.Fatalf("Error initializing data set: %v", err)
	}
	defer func() {
		postgres.postgresSession.Exec("DROP TABLE complex_test_table;")
	}()

	// run
	result, err := postgres.catTable("public", "complex_test_table", nil)

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
		if result[0].Map()[key] != value {
			t.Fatalf("Expected %v of row to equal %v (%T), but was %v (%T)", key, value, value, result[0].Map()[key], result[0].Map()[key])
		}
	}
}

func NewTestPostgres() (postgres *Postgres, err error) {
	// Create the database
	directPG, err := sql.Open("postgres", "dbname=postgres host=localhost sslmode=disable")
	if err != nil {
		return postgres, fmt.Errorf("Error connecting to Postgres to create test database: %v", err.Error())
	}

	_, err = directPG.Exec(`
    CREATE DATABASE transporter_test;
  `)

	config := map[string]interface{}{
		"type":             "postgres",
		"uri":              "host=localhost sslmode=disable dbname=transporter_test",
		"namespace":        "transporter_test.public..*",
		"replication_slot": "test_slot",
	}

	adaptor, err := Createadaptor("postgres", "", config, pipe.NewPipe(nil, "some name"))
	if err != nil {
		return postgres, err
	}

	postgres = adaptor.(*Postgres)
	return postgres, err
}

func createSimpleTestTable() (*Postgres, error) {
	postgres, err := NewTestPostgres()
	if err != nil {
		return postgres, err
	}

	_, err = postgres.postgresSession.Exec(`
    CREATE TABLE simple_test_table (
      id SERIAL PRIMARY KEY,

      colvar VARCHAR(255),
      coltimestamp TIMESTAMP
    );
  `)

	_, err = postgres.postgresSession.Exec(`
    INSERT INTO simple_test_table (colvar, coltimestamp) VALUES ('batman', now());
  `)

	return postgres, err
}

func addTestReplicationSlot(postgres *Postgres) error {
	_, err := postgres.postgresSession.Exec(`
    SELECT * FROM pg_create_logical_replication_slot('test_slot', 'test_decoding');
  `)
	return err
}

const complexTableInsert = `
     INSERT INTO complex_test_table VALUES (
        DEFAULT,             -- id
        'Wonder Woman',      -- colvar VARCHAR(255),
        now(),               -- coltimestamp TIMESTAMP,

        '{1, 2, 3, 4}',      -- colarrayint ARRAY[4],
        '{"o,ne", "two", "three", "four"}' , -- colarraystring ARRAY[4],
        4000001240124,       -- colbigint bigint,
        DEFAULT,             -- colbigserial bigserial,
        B'1',                -- colbit bit,
        false,               -- colboolean boolean,
        '(10,10),(20,20)', -- colbox box,
        E'\\xDEADBEEF',      -- colbytea bytea,
        'a',                 -- colcharacter character(1),
        '10.0.1.0/28',       -- colcidr cidr,
        '<(5, 10), 3>',      -- colcircle circle,
        now(),               -- coldate date,
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
	postgres, err := NewTestPostgres()
	if err != nil {
		return postgres, err
	}

	postgres.postgresSession.Exec("CREATE TYPE mood AS ENUM('sad', 'ok', 'happy');")

	_, err = postgres.postgresSession.Exec(`
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
		return postgres, err
	}

	_, err = postgres.postgresSession.Exec(complexTableInsert)
	return postgres, err
}
