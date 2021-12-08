package mysql

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
)

var (
	_ client.Reader = &Reader{}
)

// Reader implements the behavior defined by client.Reader for interfacing with MySQL.
type Reader struct {
}

func newReader() client.Reader {
	return &Reader{}
}

func (r *Reader) Read(resumeMap map[string]client.MessageSet, filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan client.MessageSet, error) {
		out := make(chan client.MessageSet)
		session := s.(*Session)
		go func() {
			defer close(out)
			log.With("db", session.db).Infoln("starting Read func")
			tables, err := r.listTables(session.db, session.mysqlSession, filterFn)
			if err != nil {
				log.With("db", session.db).Errorf("unable to list tables, %s", err)
				return
			}
			results := r.iterateTable(session.db, session.mysqlSession, tables, done)
			for {
				select {
				case <-done:
					return
				case result, ok := <-results:
					if !ok {
						log.With("db", session.db).Infoln("Read completed")
						return
					}
					out <- client.MessageSet{
						Msg: message.From(ops.Insert, result.table, result.data),
					}
				}
			}
		}()

		return out, nil
	}
}

func (r *Reader) listTables(db string, session *sql.DB, filterFn func(name string) bool) (<-chan string, error) {
	out := make(chan string)
	tablesResult, err := session.Query("SELECT table_schema, table_name FROM INFORMATION_SCHEMA.TABLES")
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(out)
		for tablesResult.Next() {
			var schema string
			var tname string
			err = tablesResult.Scan(&schema, &tname)
			if err != nil {
				log.With("db", db).Infoln("error scanning table name...")
				continue
			}
			name := fmt.Sprintf("%s.%s", schema, tname)
			if filterFn(name) && matchFunc(name) {
				log.With("db", db).With("table", name).Infoln("sending for iteration...")
				out <- name
			} else {
				log.With("db", db).With("table", name).Debugln("skipping iteration...")
			}
		}
		log.With("db", db).Infoln("done iterating collections")
	}()
	return out, nil
}

func matchFunc(table string) bool {
	if strings.HasPrefix(table, "information_schema.") {
		return false
	}
	return true
}

type doc struct {
	table string
	data  data.Data
}

func (r *Reader) iterateTable(db string, session *sql.DB, in <-chan string, done chan struct{}) <-chan doc {
	out := make(chan doc)
	go func() {
		defer close(out)
		for {
			select {
			case c, ok := <-in:
				if !ok {
					return
				}
				log.With("db", db).With("table", c).Infoln("iterating...")
				schemaTable := strings.Split(c, ".")
				columnsResult, err := session.Query(fmt.Sprintf(`
            SELECT COLUMN_NAME AS column_name, DATA_TYPE as data_type, "" as element_type
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE
                TABLE_SCHEMA = '%v'
            AND TABLE_NAME = '%v'
            ORDER BY ORDINAL_POSITION;
            `, schemaTable[0], schemaTable[1]))
			// No element_types in mysql since no ARRAY data type
			// we could add an empty column though to get the same layout as Postgres
				if err != nil {
					log.With("db", db).With("table", c).Errorf("error getting columns %v", err)
					continue
				}
				var columns [][]string
				for columnsResult.Next() {
					var columnName string
					var columnType string
					var columnArrayType sql.NullString // this value may be nil

					err = columnsResult.Scan(&columnName, &columnType, &columnArrayType)
					recoveredRegex := regexp.MustCompile("recovered")
					if err != nil && !recoveredRegex.MatchString(err.Error()) {
						log.With("table", c).Errorf("error scanning columns %v", err)
						continue
					}

					if columnType == "ARRAY" {
						columnType = fmt.Sprintf("%v[]", columnArrayType.String) // append [] to columnType if array
					}

					column := []string{columnName, columnType}
					columns = append(columns, column)
				}

				// build docs for table
				docsResult, _ := session.Query(fmt.Sprintf("SELECT * FROM %v", c))

				for docsResult.Next() {
					dest := make([]interface{}, len(columns))
					for i := range columns {
						dest[i] = make([]byte, 30)
						dest[i] = &dest[i]
					}

					var docMap map[string]interface{}
					err = docsResult.Scan(dest...)
					if err != nil {
						log.With("table", c).Errorf("error scanning row %v", err)
						continue
					}

					docMap = make(map[string]interface{})

					for i, value := range dest {
						switch value := value.(type) {
						case []uint8:
							docMap[columns[i][0]] = casifyValue(string(value), columns[i][1])
						case string:
							docMap[columns[i][0]] = casifyValue(string(value), columns[i][1])
						default:
							arrayRegexp := regexp.MustCompile("[[]]$")
							if arrayRegexp.MatchString(columns[i][1]) {
							} else {
								docMap[columns[i][0]] = value
							}
						}
					}
					out <- doc{table: c, data: docMap}
				}
				log.With("db", db).With("table", c).Infoln("iterating complete")
			case <-done:
				log.With("db", db).Infoln("iterating no more")
				return
			}
		}
	}()
	return out
}

// In Postgres this is in tailer.go, but since this is called even without tailing it seems like it should be here
func casifyValue(value string, valueType string) interface{} {
	findArray := regexp.MustCompile("[[]]$")

	switch {
	case value == "null":
		return nil
	case valueType == "integer" || valueType == "smallint" || valueType == "bigint":
		i, _ := strconv.Atoi(value)
		return i
	case valueType == "double precision" || valueType == "numeric" || valueType == "money":
		if valueType == "money" { // remove the dollar sign for money
			value = value[1:]
		}
		f, _ := strconv.ParseFloat(value, 64)
		return f
	case valueType == "boolean":
		return value == "true"
	case valueType == "jsonb[]" || valueType == "json":
		var m map[string]interface{}
		json.Unmarshal([]byte(value), &m)
		return m
	case len(findArray.FindAllString(valueType, 1)) > 0:
		var result []interface{}
		arrayValueType := findArray.ReplaceAllString(valueType, "")

		r := csv.NewReader(strings.NewReader(value[1 : len(value)-1]))
		arrayValues, err := r.ReadAll()
		if err != nil {
			return value
		}

		for _, arrayValue := range arrayValues[0] {
			result = append(result, casifyValue(arrayValue, arrayValueType))
		}

		return result
	case valueType == "timestamp without time zone":
		// parse time like 2015-08-21 16:09:02.988058
		t, err := time.Parse("2006-01-02 15:04:05.9", value)
		if err != nil {
			fmt.Printf("\nTime (%v) parse error: %v\n\n", value, err)
		}
		return t
	case valueType == "date":
		t, err := time.Parse("2006-01-02", value)
		if err != nil {
			fmt.Printf("\nTime (%v) parse error: %v\n\n", value, err)
		}
		return t
	}

	return value
}
