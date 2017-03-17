package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

var _ client.Writer = &Writer{}

// Writer implements client.Writer for use with MongoDB
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
			log.Infof("no function registered for operation, %s\n", msg.OP())
			return msg, nil
		}
		return msg, writeFunc(msg, s.(*Session).pqSession)
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
		placeholders = append(placeholders, fmt.Sprintf("$%v", i))

		switch value.(type) {
		case map[string]interface{}:
			value, _ = json.Marshal(value)
		case []interface{}:
			value, _ = json.Marshal(value)
		}
		data = append(data, value)

		i = i + 1
	}

	query := fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v);", m.Namespace(), strings.Join(keys, ", "), strings.Join(placeholders, ", "))
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
		case map[string]interface{}:
			value, _ = json.Marshal(value)
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
		case map[string]interface{}:
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
