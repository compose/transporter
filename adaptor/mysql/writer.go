package mysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/compose/mejson"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkt"
)

var _ client.Writer = &Writer{}

// Writer implements client.Writer for use with MySQL
type Writer struct {
	writeMap map[ops.Op]func(message.Msg, *sql.DB) error
}

func newWriter() *Writer {
	w := &Writer{}
	w.writeMap = map[ops.Op]func(message.Msg, *sql.DB) error{
		ops.Insert: insertMsg,
		ops.Update: updateMsg,
		ops.Delete: deleteMsg,
	}
	return w
}

func (w *Writer) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		writeFunc, ok := w.writeMap[msg.OP()]
		if !ok {
			log.Infof("no function registered for operation, %s", msg.OP())
			if msg.Confirms() != nil {
				msg.Confirms() <- struct{}{}
			}
			return msg, nil
		}
		if err := writeFunc(msg, s.(*Session).mysqlSession); err != nil {
			return nil, err
		}
		if msg.Confirms() != nil {
			msg.Confirms() <- struct{}{}
		}
		return msg, nil
	}
}

func insertMsg(m message.Msg, s *sql.DB) error {
	log.With("table", m.Namespace()).Debugln("INSERT")
	var (
		keys         []string
		placeholders []string
		data         []interface{}
	)

	i := 1
	for key, value := range m.Data() {
		keys = append(keys, key)
		// Mysql uses "?, ?, ?" instead of "$1, $2, $3"
		// Wrap placeholder for geometry types
		// Overkill using switch/case for just geometry,
		// but there might be other types we need to handle
		placeholder := "?"
		switch value.(type) {
			case *geom.Point, *geom.LineString, *geom.Polygon, *geom.GeometryCollection:
				// Wrap in ST_GeomFromText
				// Supposedly not required in "later" MySQLs
				// Although the format changes, e.g. `POINT (15,15)` vs WKT of `POINT (15 15)`
				// So might as well stick with it. Possible performance impact?
				// We could use binary `ST_GeomFromWKB` though
				placeholder = "ST_GeomFromText(?)"
		}
		placeholders = append(placeholders, placeholder)

		log.Debugf("Type of value is %T", value)
		switch value.(type) {
			// Can add others here such as binary and bit, etc if needed
			case *geom.Point, *geom.LineString, *geom.Polygon, *geom.GeometryCollection:
				value, _ = wkt.Marshal(value.(geom.T))
				value = value.(string)
			case time.Time:
				// MySQL can write this format into DATE, DATETIME and TIMESTAMP
				value = value.(time.Time).Format("2006-01-02 15:04:05.000000")
			case map[string]interface{}, mejson.M, []map[string]interface{}, mejson.S:
				value, _ = json.Marshal(value)
			case []interface{}:
				value, _ = json.Marshal(value)
				value = string(value.([]byte))
				value = fmt.Sprintf("{%v}", value.(string)[1:len(value.(string))-1])
		}
		data = append(data, value)

		i = i + 1
	}

	query := fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v);", m.Namespace(), strings.Join(keys, ", "), strings.Join(placeholders, ", "))
	log.Debugf("query: %s", query)
	log.Debugf("data: %s", data)

	// TODO: Figure out finding the log level so we only run this bit in debug
	//if log.level == "debug" {
	//	for i := 0; i < len(data); i++ {
	//		log.With("table", m.Namespace()).Debugf("data: %s", data[i])
	//	}
	//}
	// INSERT INTO writer_insert_test.simple_test_table (id, colvar, coltimestamp) VALUES ($1, $2, $3);
	_, err := s.Exec(query, data...)
	return err
}

func deleteMsg(m message.Msg, s *sql.DB) error {
	log.With("table", m.Namespace()).With("values", m.Data()).Debugln("DELETE")
	var (
		ckeys []string
		vals  []interface{}
	)
	pkeys, err := primaryKeys(m.Namespace(), s)
	if err != nil {
		return err
	}
	i := 1
	for key, value := range m.Data() {
		if pkeys[key] { // key is primary key
			ckeys = append(ckeys, fmt.Sprintf("%v = ?", key))
		}
		switch value.(type) {
			case map[string]interface{}, mejson.M, []map[string]interface{}, mejson.S:
				value, _ = json.Marshal(value)
			case []interface{}:
				value, _ = json.Marshal(value)
				value = string(value.([]byte))
				value = fmt.Sprintf("{%v}", value.(string)[1:len(value.(string))-1])
		}
		vals = append(vals, value)
		i = i + 1
	}

	if len(pkeys) != len(ckeys) {
		return fmt.Errorf("All primary keys were not accounted for. Provided: %v; Required; %v", ckeys, pkeys)
	}

	query := fmt.Sprintf("DELETE FROM %v WHERE %v;", m.Namespace(), strings.Join(ckeys, " AND "))
	log.Debugf("query: %s", query)
	log.Debugf("vals: %s", vals)
	_, err = s.Exec(query, vals...)
	return err
}

func updateMsg(m message.Msg, s *sql.DB) error {
	log.With("table", m.Namespace()).Debugln("UPDATE")
	var (
		ckeys []string
		ukeys []string
		cvals []interface{}
		uvals []interface{}
		vals  []interface{}
	)

	pkeys, err := primaryKeys(m.Namespace(), s)
	if err != nil {
		return err
	}

	i := 1
	for key, value := range m.Data() {
		// Mysql uses "?, ?, ?" instead of "$1, $2, $3"
		// Wrap placeholder for geometry types
		// Overkill using switch/case for just geometry,
		// but there might be other types we need to handle
		placeholder := "?"
		switch value.(type) {
			case *geom.Point, *geom.LineString, *geom.Polygon, *geom.GeometryCollection:
				// Wrap in ST_GeomFromText
				// Supposedly not required in "later" MySQLs
				// Although the format changes, e.g. `POINT (15,15)` vs WKT of `POINT (15 15)`
				// So might as well stick with it. Possible performance impact?
				// We could use binary `ST_GeomFromWKB` though
				placeholder = "ST_GeomFromText(?)"
		}
		if pkeys[key] { // key is primary key
			ckeys = append(ckeys, fmt.Sprintf("%v=%s", key, placeholder))
		} else {
			ukeys = append(ukeys, fmt.Sprintf("%v=%s", key, placeholder))
		}

		switch value.(type) {
			// Can add others here such as binary and bit, etc if needed
			case *geom.Point, *geom.LineString, *geom.Polygon, *geom.GeometryCollection:
				value, _ = wkt.Marshal(value.(geom.T))
				value = value.(string)
			case time.Time:
				// MySQL can write this format into DATE, DATETIME and TIMESTAMP
				value = value.(time.Time).Format("2006-01-02 15:04:05.000000")
			case map[string]interface{}, mejson.M, []map[string]interface{}, mejson.S:
				value, _ = json.Marshal(value)
			case []interface{}:
				value, _ = json.Marshal(value)
				value = string(value.([]byte))
				value = fmt.Sprintf("{%v}", value.(string)[1:len(value.(string))-1])
		}
		// if it's a primary key it needs to go at the end of the vals list
		// So perhaps easier to do cvals and uvals and then combine at end
		if pkeys[key] {
			cvals = append(cvals, value)
		} else {
			uvals = append(uvals, value)
		}
		i = i + 1
	}

	// Join vals
	vals = append(uvals, cvals...)

	if len(pkeys) != len(ckeys) {
		return fmt.Errorf("All primary keys were not accounted for. Provided: %v; Required; %v", ckeys, pkeys)
	}

	query := fmt.Sprintf("UPDATE %v SET %v WHERE %v;", m.Namespace(), strings.Join(ukeys, ", "), strings.Join(ckeys, " AND "))
	// Note: For Postgresql this results in:
	//
	// UPDATE writer_update_test.update_test_table SET colvar=$2, coltimestamp=$3 WHERE id=$1; 
	//
	// which is wrong for MySQL, need just `?`
	//
	log.Debugf("query: %s", query)
	log.Debugf("vals: %s", vals)
	_, err = s.Exec(query, vals...)
	return err
}

func primaryKeys(namespace string, db *sql.DB) (primaryKeys map[string]bool, err error) {
	primaryKeys = map[string]bool{}
	namespaceArray := strings.SplitN(namespace, ".", 2)
	var (
		tableSchema string
		tableName   string
		columnName  string
	)
	if namespaceArray[1] == "" {
		tableSchema = "public"
		tableName = namespaceArray[0]
	} else {
		tableSchema = namespaceArray[0]
		tableName = namespaceArray[1]
	}

	// Need to update this
	// unexpected Update error, Error 1109: Unknown table 'constraint_column_usage' in information_schema
	//
	// This returns something like:
	//
	//  column_name
	// -------------
	// recipe_id
	// recipe_rating
	// (2 rows)
	//
	// Below from here: https://stackoverflow.com/a/12379241/208793
	tablesResult, err := db.Query(fmt.Sprintf(`
		SELECT k.COLUMN_NAME
		FROM information_schema.table_constraints t
		LEFT JOIN information_schema.key_column_usage k
		USING(constraint_name,table_schema,table_name)
		WHERE t.constraint_type='PRIMARY KEY'
			AND t.table_schema='%v'
			AND t.table_name='%v'
	`, tableSchema, tableName))
	if err != nil {
		return primaryKeys, err
	}

	for tablesResult.Next() {
		err = tablesResult.Scan(&columnName)
		if err != nil {
			return primaryKeys, err
		}
		primaryKeys[columnName] = true
	}

	return primaryKeys, err
}
