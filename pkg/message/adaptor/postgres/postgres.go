package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"git.compose.io/compose/transporter/pkg/message"
	"git.compose.io/compose/transporter/pkg/message/data"
	"git.compose.io/compose/transporter/pkg/message/ops"
)

type Adaptor struct {
	session *sql.DB
}

var _ message.Adaptor = Adaptor{}
var _ message.Insertable = Adaptor{}
var _ message.Deletable = Adaptor{}
var _ message.Updatable = Adaptor{}

func init() {
	a := Adaptor{}
	message.Register(a.Name(), a)
}

func (r Adaptor) Name() string {
	return "postgres"
}

func (r Adaptor) From(op ops.Op, namespace string, d data.Data) message.Msg {
	return &Message{
		Operation: op,
		TS:        time.Now().Unix(),
		NS:        namespace,
		MapData:   d,
	}
}

func (r Adaptor) Insert(m message.Msg) error {
	fmt.Printf("Write INSERT to Postgres %v\n", m.Namespace())
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
	_, err := r.session.Exec(query, data...)
	return err
}

func (r Adaptor) Delete(m message.Msg) error {
	fmt.Printf("Write DELETE to Postgres %v values %v\n", m.Namespace(), m.Data())
	var (
		ckeys []string
		vals  []interface{}
	)
	pkeys, err := r.primaryKeys(m.Namespace())
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
	_, err = r.session.Exec(query, vals...)
	return err
}

func (r Adaptor) Update(m message.Msg) error {
	fmt.Printf("Write UPDATE to Postgres %v\n", m.Namespace())
	var (
		ckeys []string
		ukeys []string
		vals  []interface{}
	)

	pkeys, err := r.primaryKeys(m.Namespace())
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
	_, err = r.session.Exec(query, vals...)
	return err
}

func (r Adaptor) UseSession(session *sql.DB) Adaptor {
	r.session = session
	return r
}

func (r Adaptor) primaryKeys(namespace string) (primaryKeys map[string]bool, err error) {
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

	tablesResult, err := r.session.Query(fmt.Sprintf(`
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
