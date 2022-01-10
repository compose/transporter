package mysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

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
		placeholders = append(placeholders, "?")

		// TODO: Remove debugging/developing stuff:
		fmt.Printf("Type of value is %T\n", value)
		switch value.(type) {
		// Can add others here such as binary and bit, etc if needed
		case *geom.Point, *geom.LineString, *geom.Polygon, *geom.GeometryCollection:
			// Wrap in ST_GeomFromText
			// Supposedly not required in "later" MySQLs
			// Although it's still safe to use. Possible performance impact?
			value, _ = wkt.Marshal(value.(geom.T))
			value = "ST_GeomFromText('"+value.(string)+"')"
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
	// TODO: Remove debugging/developing stuff:
	log.Infoln(query)
	log.Infoln(data)
	for i := 0; i < len(data); i++ {
		log.Infoln(data[i])
	}
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
			ckeys = append(ckeys, fmt.Sprintf("%v = $%v", key, i))
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
	_, err = s.Exec(query, vals...)
	return err
}

func updateMsg(m message.Msg, s *sql.DB) error {
	log.With("table", m.Namespace()).Debugln("UPDATE")
	var (
		ckeys []string
		ukeys []string
		vals  []interface{}
	)

	pkeys, err := primaryKeys(m.Namespace(), s)
	if err != nil {
		return err
	}

	i := 1
	for key, value := range m.Data() {
		if pkeys[key] { // key is primary key
			ckeys = append(ckeys, fmt.Sprintf("%v=$%v", key, i))
		} else {
			ukeys = append(ukeys, fmt.Sprintf("%v=$%v", key, i))
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

	query := fmt.Sprintf("UPDATE %v SET %v WHERE %v;", m.Namespace(), strings.Join(ukeys, ", "), strings.Join(ckeys, " AND "))
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

	tablesResult, err := db.Query(fmt.Sprintf(`
		SELECT
			column_name
		FROM information_schema.table_constraints constraints
			INNER JOIN information_schema.constraint_column_usage column_map
				ON column_map.constraint_name = constraints.constraint_name
		WHERE constraints.constraint_type = 'PRIMARY KEY'
			AND constraints.table_schema = '%v'
			AND constraints.table_name = '%v'
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
