package postgres

import (
	"database/sql"
	"fmt"
	"regexp"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
)

var (
	_ client.Reader = &Reader{}
)

// Reader implements the behavior defined by client.Reader for interfacing with MongoDB.
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
			table, err := r.listTables(session.db, session.pqSession, filterFn)
			if err != nil {
				log.With("db", session.db).Errorf("unable to list tables, %s", err)
				return
			}
			results := r.iterateTable(session.db, session.pqSession, table, done)
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

type table struct {
	schema string
	name   string
}

func (t table) String() string {
	return fmt.Sprintf("%s.\"%s\"", t.schema, t.name)
}

func (r *Reader) listTables(db string, session *sql.DB, filterFn func(name string) bool) (<-chan *table, error) {
	out := make(chan *table)
	tablesResult, err := session.Query("SELECT table_schema,table_name FROM information_schema.tables")
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(out)
		for tablesResult.Next() {
			table := &table{}
			err = tablesResult.Scan(&table.schema, &table.name)
			if err != nil {
				log.With("db", db).Infoln("error scanning table name...")
				continue
			}
			if filterFn(table.String()) && matchFunc(table.schema) {
				log.With("db", db).With("table", table).Infoln("sending for iteration...")
				out <- table
			} else {
				log.With("db", db).With("table", table).Debugln("skipping iteration...")
			}
		}
		log.With("db", db).Infoln("done iterating collections")
	}()
	return out, nil
}

func matchFunc(schema string) bool {
	return schema != "information_schema" && schema != "pg_catalog"
}

type doc struct {
	table string
	data  data.Data
}

func (r *Reader) iterateTable(db string, session *sql.DB, in <-chan *table, done chan struct{}) <-chan doc {
	out := make(chan doc)
	go func() {
		defer close(out)
		for {
			select {
			case table, ok := <-in:
				if !ok {
					return
				}
				log.With("db", db).With("table", table).Infoln("iterating...")
				columnsResult, err := session.Query(fmt.Sprintf(`
            SELECT c.column_name, c.data_type, e.data_type AS element_type
            FROM information_schema.columns c LEFT JOIN information_schema.element_types e
                 ON ((c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier)
                   = (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier))
            WHERE c.table_schema = '%v' AND c.table_name = '%v'
            ORDER BY c.ordinal_position;
            `, table.schema, table.name))
				if err != nil {
					log.With("db", db).With("table", table).Errorf("error getting columns %v", err)
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
						log.With("table", table).Errorf("error scanning columns %v", err)
						continue
					}

					if columnType == "ARRAY" {
						columnType = fmt.Sprintf("%v[]", columnArrayType.String) // append [] to columnType if array
					}

					column := []string{columnName, columnType}
					columns = append(columns, column)
				}

				// build docs for table
				docsResult, err := session.Query(fmt.Sprintf("SELECT * FROM %v", table))

				for docsResult.Next() {
					dest := make([]interface{}, len(columns))
					for i := range columns {
						dest[i] = make([]byte, 30)
						dest[i] = &dest[i]
					}

					var docMap map[string]interface{}
					err = docsResult.Scan(dest...)
					if err != nil {
						log.With("table", table).Errorf("error scanning row %v", err)
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
					out <- doc{table: table.String(), data: docMap}
				}
				log.With("db", db).With("table", table).Infoln("iterating complete")
			case <-done:
				log.With("db", db).Infoln("iterating no more")
				return
			}
		}
	}()
	return out
}
