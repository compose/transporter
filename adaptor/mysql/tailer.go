package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/commitlog"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"

	// Naming conflict with Transporter adaptor itself
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

var (
	_ client.Reader = &Tailer{}
)

// Tailer implements the behaviour defined by client.Tailer for interfacing with the MySQL binlog.
// We'll have to pass through the dsn so that we can use it to configure the sync client
type Tailer struct {
	reader client.Reader
	dsn    string
}

func newTailer(dsn string) client.Reader {
	return &Tailer{newReader(), dsn}
}

// Tail does the things
func (t *Tailer) Read(resumeMap map[string]client.MessageSet, filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan client.MessageSet, error) {
		// How is resuming supposed to work?
		readFunc := t.reader.Read(resumeMap, filterFn)
		msgChan, err := readFunc(s, done)
		if err != nil {
			return nil, err
		}
		session := s.(*Session)

		// TODO: This could go in a separate function and return a cfg?
		parsedDSN, err:= url.Parse(t.dsn)
		if err != nil {
			return nil, err
		}
		host := parsedDSN.Hostname()
		port := parsedDSN.Port()
		portInt, err:= strconv.Atoi(port)
		if err != nil {
			return nil, err
		}
		user := parsedDSN.User.Username()
		pass, _ := parsedDSN.User.Password()
		// Not needed?
		//path := parsedDSN.Path[1:]
		scheme := parsedDSN.Scheme

		// Find binlog info
		var binFile string
		var binPosition int
		var _binBinlogDoDB string
		var _binBinlogIgnoreDB string
		var _binExecutedGtidSet string
		result := session.mysqlSession.QueryRow("SHOW MASTER STATUS")
		// We need to scan all columns... even though we don't care about them all.
		// mysql> show master status;
		// +-------------------+----------+--------------+------------------+-------------------------------------------+
		// | File              | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set                         |
		// +-------------------+----------+--------------+------------------+-------------------------------------------+
		// | master-bin.000001 |   163739 |              |                  | a852989a-1894-4fcb-a060-a4aaaf06b9f0:1-55 |
		// +-------------------+----------+--------------+------------------+-------------------------------------------+
		// 1 row in set (0.04 sec)
		//
		scanErr := result.Scan(&binFile, &binPosition, &_binBinlogDoDB, &_binBinlogIgnoreDB, &_binExecutedGtidSet)
		log.Debugf("binFile: %s, binPosition: %d", binFile, binPosition)
		if scanErr != nil {
			// Quit gracefully since can't tail?
			log.Errorln("Can't find binFile or binPosition. Unable to tail")
			os.Exit(1)
		}

		// Find serverID
		var serverID uint32
		result = session.mysqlSession.QueryRow("SELECT @@server_id as SERVER_ID")
		scanErr = result.Scan(&serverID)
		if scanErr != nil {
			// Quit gracefully since can't tail?
			log.Errorln("Can't find source server ID")
			os.Exit(1)
		}

		// Configure sync client
		cfg := replication.BinlogSyncerConfig{
			ServerID: serverID,
			Flavor:   scheme,
			Host:     host,
			Port:     uint16(portInt),
			User:     user,
			Password: pass,
		}

		// Create syncer
		syncer := replication.NewBinlogSyncer(cfg)

		// Start streamer
		streamer, _ := syncer.StartSync(gomysql.Position{Name: binFile, Pos: uint32(binPosition)})
		// How to properly close this?
		// There is no EndSync, but there is a close we can call on the `done` channel

		out := make(chan client.MessageSet)
		// Will we have to pass things (such as streamer) into this function?
		go func() {
			defer close(out)
			// read until reader done
			for msg := range msgChan {
				out <- msg
			}
			// start tailing/streaming
			log.With("db", session.db).Infoln("Listening for changes...")
			for {
				// Use timeout context (for now at least)
				// If we are using a timeout I think we can happily sit there for a bit
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				select {
				// Notes to self on what this is doing...
				// From reading around, e.g: https://golangbyexample.com/select-statement-golang/
				// I _think_ the blocking 1 sec sleep is there just to give the "done" channel a chance to
				// execute otherwise there is no guarantee it would close because the "tailing"
				// channel could also be executing and if both are ready it'll select one at random.
				// For Postgresql this works because each call pulls all the logical decoding messages
				// since the last call.
				// For MySQL this isn't going to work correctly because we are pulling/streaming one
				// event at a time. A 1 second sleep is no good.
				// Historically, way back, channels weren't used:
				//
				// - https://github.com/compose/transporter/pull/281/files
				// - https://github.com/compose/transporter/blob/7875ce0a2343fe94d7d6f9703e2e578cd6b77cba/pkg/adaptor/postgres/postgres.go#L305-L318
				//
				// We need to stick with channels, but need to do this a bit differently
				// Can we do outside of the select/case?
				// Unless we can use DumpEvents instead of GetEvent?
				// Or we use default? That way it doesn't block but should still close
				case <-done:
					log.With("db", session.db).Infoln("tailing stopping...")
					syncer.Close()
					// ?? this doesn't use the cancel above. Need to fix.
					return
				default:
					// This blocks until an event is received which will still prevent the done channel from executing so use a timeout
					event, ctxerr := streamer.GetEvent(ctx)
					// Can't easily use below with `log.` so leaving commented out for debugging
					//event.Dump(os.Stdout)

					// Do not really understand this next bit yet
					// Cancels existing/current context?
					cancel()
					if ctxerr == context.DeadlineExceeded {
						// Allow `done` to execute
						continue
					}

					msgSlice, skip, err := t.processEvent(s, event, filterFn)
					if err != nil {
						log.With("db", session.db).Errorf("error processing event from binlog %v", err)
						continue
					}
					// send processed events to the channel
					// What if there is an event we want to skip? Need a way to process that?
					if skip {
						log.With("db", session.db).Debugf("skipping event from binlog %v", msgSlice)
						continue
					}
					for _, msg := range msgSlice {
						out <- msg
					}
				}
			}
		}()

		return out, nil
	}
}

// For a statement like this:
//
//    INSERT INTO recipes (recipe_id, recipe_name) VALUES (1,'Tacos'), (2,'Tomato Soup'), (3,'Grilled Cheese');
// Postgresql has multiple events split per logical decoding rows:
//
//    0/500CEC8 | 496 | table public.recipes: INSERT: recipe_id[integer]:1 recipe_name[character varying]:'Tacos' recipe_rating[integer]:null
//    0/500D050 | 496 | table public.recipes: INSERT: recipe_id[integer]:2 recipe_name[character varying]:'Tomato Soup' recipe_rating[integer]:null
//    0/500D120 | 496 | table public.recipes: INSERT: recipe_id[integer]:3 recipe_name[character varying]:'Grilled Cheese' recipe_rating[integer]:null
//
// MySQL has one binlog event containing multiple updates ("Same, but different")
//
// [[1 Tacos] [2 Tomato Soup] [3 Grilled Cheese]]
//
// It seems we do not get the column names, instead we'll get `<nil>` if a column is skipped
// This is unfortunate for our use case as we'll have to fill in the column names
//
// For Postgresql, a string like this from logical decoding:
//
//     "id[integer]:1 data[text]:'1'"
//
// Will end up like:
//
//     map[data:1 id:1]
//
// So we need to get MySQL stuff in that format.
//
// Note: Canal has a lot of depth for MySQL sync that we (fortunately! For me!) don't need to handle in Transporter (which is more breadth than depth)
func (t *Tailer) processEvent(s client.Session, event *replication.BinlogEvent, filterFn client.NsFilterFunc) ([]client.MessageSet, bool, error) {
	var (
		result        []client.MessageSet
		skip          = false
		err           error
		action        ops.Op
		schema, table string
	)

	// We are basically copying this from the following, but there's not really a different way to write these:
	//
	// - https://github.com/go-mysql-org/go-mysql/blob/d1666538b005e996414063695ca223994e9dc19d/canal/sync.go#L91-L172
	// - https://github.com/go-mysql-org/go-mysql/blob/b4f7136548f0758730685ebd78814eb3e5e4b0b0/canal/sync.go#L248-L272
	switch event.Event.(type) {
	case *replication.RowsEvent:
		// Need to cast
		rowsEvent := event.Event.(*replication.RowsEvent)
		log.Debugln("Logging rowsEvent:")
		log.Debugln(rowsEvent)
		// We only care about Insert / Update / Delete
		// 1. Schema
		schema = string(rowsEvent.Table.Schema)
		// 2. Table
		table = string(rowsEvent.Table.Table)
		// Make sure we are getting changes on valid tables
		schemaAndTable := fmt.Sprintf("%v.%v", schema, table)
		if !filterFn(schemaAndTable) {
			skip = true
			// TODO: Do we need to configure an empty result?
			return result, skip, fmt.Errorf("Error processing action from string: %v", rowsEvent.Rows)
		}
		// 3. Action (Insert / Update / Delete)
		switch event.Header.EventType {
		case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
			action = ops.Insert
		case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			action = ops.Delete
		case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			action = ops.Update
			// For an update MySQL binlog returns a before vs after, but we just need the after
			// I.e. this:
			//
			//     mysql> update recipes set recipe_name = 'Nachos' where recipe_id = 1;
			//
			// results in:
			//
			//     [[1 Tacos ] [1 Nachos ]]
			//
		default:
			// TODO: Do we want to skip? Or just Error?
			return result, skip, fmt.Errorf("Error processing action from string: %v", rowsEvent.Rows)
		}
		// Fetch column / data-type info before we can do 4.

		session := s.(*Session)
		// Copied from reader.go `iterateTable`
		// TODO: Use a common function for both
		// TODO: Do we really want to do this _every_ time? Seems ultra inefficient
		columnsResult, err := session.mysqlSession.Query(fmt.Sprintf(`
                SELECT COLUMN_NAME AS column_name, DATA_TYPE as data_type, "" as element_type
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE
                    TABLE_SCHEMA = '%v'
                AND TABLE_NAME = '%v'
                ORDER BY ORDINAL_POSITION;
                `, schema, table))
		// No element_types in mysql since no ARRAY data type
		// at the moment we add an empty column to get the same layout as Postgres
		// TODO: Update this code so we don't need that empty column?
		if err != nil {
			log.With("schema", schema).With("table", table).Errorf("error getting columns %v", err)
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

			column := []string{columnName, columnType}
			columns = append(columns, column)
			log.With("db", session.db).Debugln(columnName + ": " + columnType)
		}
		// 4. Remaining stuff / data
		for i, row := range rowsEvent.Rows {
			// This is the tricky bit!

			log.With("op", action).With("table", schemaAndTable).Debugln("received")

			// Skip first row for an update
			if i == 0 && action == ops.Update {
				continue
			}

			// TODO: We might want to take advantage of `handleUnsigned`:
			//
			// https://github.com/go-mysql-org/go-mysql/blob/b4f7136548f0758730685ebd78814eb3e5e4b0b0/canal/rows.go#L46

			docMap := parseEventRow(columns, row)
			result = append(result, client.MessageSet{
				Msg:  message.From(action, schemaAndTable, docMap),
				Mode: commitlog.Sync,
			})
		}
	default:
		skip = true
	}

	return result, skip, err
}

func parseEventRow(columns [][]string, d []interface{}) data.Data {
	// The main issue with MySQL is that we don't get the column names!!! So we need to fill those in...
	// We can use `TableMapEvent`s or Transporter itself since it has read the table. `iterateTable`?

	// See reader.go
	// out <- doc{table: c, data: docMap}
	// docMap[columns[i][0]] = value

	data := make(data.Data)

	// I think basically need to merge `iterateTable` with the data from the binlog.

	// row = [1 Tacos]

	// Might not need any of this dest stuff...
	// Since that is for scanning into and we don't need to do that
	//dest := make([]interface{}, len(columns))
	//for i := range columns {
	//	dest[i] = make([]byte, 30)
	//	dest[i] = &dest[i]
	//}

	// Using data instead
	//var docMap map[string]interface{}

	// We don't need to Scan, we have the data already
	//err = docsResult.Scan(dest...)
	//if err != nil {
	//	log.With("table", c).Errorf("error scanning row %v", err)
	//	continue
	//}

	//Using data instead
	//docMap = make(map[string]interface{})

	for i, value := range d {
		log.Debugln("Logging value from parseEventRow:")
		log.Debugln(value)
		xType := fmt.Sprintf("%T", value)
		log.Debugln("Logging type from parseEventRow:")
		log.Debugln(xType)
		switch value := value.(type) {
		// Seems everything is []uint8
		case []uint8:
			data[columns[i][0]] = casifyValue(string(value), columns[i][1])
		case string:
			data[columns[i][0]] = casifyValue(string(value), columns[i][1])
		default:
			// TODO: This is probably a Postgresql thing and needs removing here and in reader.go
			arrayRegexp := regexp.MustCompile("[[]]$")
			if arrayRegexp.MatchString(columns[i][1]) {
			} else {
				data[columns[i][0]] = value
			}
		}
	}

	// Any difference between docMap and data in this reader context?
	// Data is `map[string]interface{}`
	// So it's the same
	return data
}
