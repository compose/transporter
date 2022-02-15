package mysql

import (
	"context"
	"fmt"
	//"regexp"
	"strconv"
	//"strings"
	"time"
	"net/url"

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
	reader          client.Reader
	dsn             string
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

		// This could go in a separate function and return a cfg?
		parsedDSN, _ := url.Parse(t.dsn)
		host := parsedDSN.Hostname()
		port := parsedDSN.Port()
		portInt, _ := strconv.Atoi(port)
		user := parsedDSN.User.Username()
		pass, _ := parsedDSN.User.Password()
		// Not needed?
		//path := parsedDSN.Path[1:]
		scheme := parsedDSN.Scheme

		// Find binlog info
		var binFile string
		var binPosition int
		result, err := session.mysqlSession.Query("SHOW MASTER STATUS")
		result.Scan(&binFile, &binPosition)

		// Configure sync client
		cfg := replication.BinlogSyncerConfig {
			// Needs an actual ServerID
			ServerID: 100,
			Flavor:   scheme,
			Host:     host,
			Port:     uint16(portInt),
			User:     user,
			Password: pass,
		}

		// Create syncer
		syncer := replication.NewBinlogSyncer(cfg)

		// Start streamer
		streamer, _ := syncer.StartSync(gomysql.Position{binFile, uint32(binPosition)})
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
					return
				default:
					// This blocks until an event is received which will still prevent the done channel from executing so use a timeout
					event, ctxerr := streamer.GetEvent(ctx)
					// Do not really understand this next bit yet
					// Cancels existing/current context?
					cancel()
					if ctxerr == context.DeadlineExceeded {
						// Allow `done` to execute
						continue
					}
					// TODO, we need to handle rotation of the binlog file...
					// E.g: https://github.com/go-mysql-org/go-mysql/blob/d1666538b005e996414063695ca223994e9dc19d/canal/sync.go#L60-L64

					msg, skip, err := t.processEvent(event, filterFn)
					if err != nil {
						log.With("db", session.db).Errorf("error processing event from binlog %v", err)
						continue
					}
					// send processed events to the channel
					// What if there is an event we want to skip? Need a way to process that?
					if skip {
						log.With("db", session.db).Debugf("skipping event from binlog %v", msg)
					} else {
						out <- msg
					}
				}
			}
		}()

		return out, nil
	}
}

// In Postgresql this returns multiple events. For MySQL it will just be one
// This _might_ end up being merged with parseData as a result
// Canal has a lot of depth for MySQL sync that we (fortunately! For me!) don't need to handle in Transporter (which is more breadth than depth)
func (t *Tailer) processEvent(event *replication.BinlogEvent, filterFn client.NsFilterFunc) (client.MessageSet, bool, error) {
	var (
		result client.MessageSet
		skip = false
		err error
		action ops.Op
		schema, table string
		eventData [][]interface {}
	)

	// TODO: Handle rotate events here or in Read?

	// We are basically copying this from the following, but there's not really a different way to write these:
	//
	// - https://github.com/go-mysql-org/go-mysql/blob/d1666538b005e996414063695ca223994e9dc19d/canal/sync.go#L91-L172
	// - https://github.com/go-mysql-org/go-mysql/blob/b4f7136548f0758730685ebd78814eb3e5e4b0b0/canal/sync.go#L248-L272
	switch event.Event.(type) {
		case *replication.RowsEvent:
			// Need to cast
			rowsEvent := event.Event.(*replication.RowsEvent)
			// We only care about Insert / Update / Delete
			// 1. Schema
			schema = string(rowsEvent.Table.Schema)
			// 2. Table
			table = string(rowsEvent.Table.Table)
			// 3. Action (Insert / Update / Delete)
			switch event.Header.EventType {
				case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
					action = ops.Insert
				case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
					action = ops.Delete
				case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
					action = ops.Update
				default:
					return result, skip, fmt.Errorf("Error processing action from string: %v", rowsEvent.Rows)
			}
			// 4. Remaining stuff / data
			eventData = rowsEvent.Rows
			// This is the tricky bit!
			//
			// I don't fully get why this is called "Rows" (plural) and not "Row" if it's just one update, but anyway...
			//
			// A statement like this:
			//
			//     INSERT INTO recipes (recipe_id, recipe_name) VALUES (1,'Tacos'), (2,'Tomato Soup'), (3,'Grilled Cheese');
			//
			// Would result in:
			//
			//     [[1 Tacos] [2 Tomato Soup] [3 Grilled Cheese]]
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
		default:
			skip = true
	}

	// Make sure we are getting changes on valid tables
	schemaAndTable := fmt.Sprintf("%v.%v", schema, table)
	if !filterFn(schemaAndTable) {
		skip = true
	}

	log.With("op", action).With("table", schemaAndTable).Debugln("received")

	// We might want to take advantage of `handleUnsigned`:
	//
	// https://github.com/go-mysql-org/go-mysql/blob/b4f7136548f0758730685ebd78814eb3e5e4b0b0/canal/rows.go#L46
	//
	docMap := parseEventData(eventData)
	result = client.MessageSet{
		Msg:  message.From(action, schemaAndTable, docMap),
		Mode: commitlog.Sync,
	}

	return result, skip, err
}

func parseEventData(d [][]interface {}) data.Data {
	// The main issue with MySQL is that we don't get the column names!!! So we need to fill those in
	data := make(data.Data)

	var (
		label                  string
		labelFinished          bool
		valueType              string
		valueTypeFinished      bool
		openBracketInValueType bool
		skippedColon           bool
		value                  string // will type switch later
		valueEndCharacter      string
		deferredSingleQuote    bool
		valueFinished          bool
	)

	valueTypeFinished = false
	labelFinished = false
	skippedColon = false
	deferredSingleQuote = false
	openBracketInValueType = false
	valueFinished = false

	// [[1 Tacos] [2 Tomato Soup] [3 Grilled Cheese]]
	// These are rough initial changes just to get it to build so I can commit the changes for `processEvent`
	// Hence also very bad formatting / indentation
	for _, row := range d {
		for _, item := range row {

	for _, character := range item.(string) {
		if !labelFinished {
			if string(character) == "[" {
				labelFinished = true
				continue
			}
			label = fmt.Sprintf("%v%v", label, string(character))
			continue
		}
		if !valueTypeFinished {
			if openBracketInValueType && string(character) == "]" { // if a bracket is open, close it
				openBracketInValueType = false
			} else if string(character) == "]" { // if a bracket is not open, finish valueType
				valueTypeFinished = true
				continue
			} else if string(character) == "[" {
				openBracketInValueType = true
			}
			valueType = fmt.Sprintf("%v%v", valueType, string(character))
			continue
		}

		if !skippedColon && string(character) == ":" {
			skippedColon = true
			continue
		}

		if len(valueEndCharacter) == 0 {
			if string(character) == "'" {
				valueEndCharacter = "'"
				continue
			}
			valueEndCharacter = " "
		}

		// ending with '
		if deferredSingleQuote && string(character) == " " { // we hit an unescaped single quote
			valueFinished = true
		} else if deferredSingleQuote && string(character) == "'" { // we hit an escaped single quote ''
			deferredSingleQuote = false
		} else if string(character) == "'" && !deferredSingleQuote { // we hit a first single quote
			deferredSingleQuote = true
			continue
		}

		// ending with space
		if valueEndCharacter == " " && string(character) == valueEndCharacter {
			valueFinished = true
		}

		// continue parsing
		if !valueFinished {
			value = fmt.Sprintf("%v%v", value, string(character))
			continue
		}

		// Set and reset
		data[label] = casifyValue(value, valueType)

		label = ""
		labelFinished = false
		valueType = ""
		valueTypeFinished = false
		skippedColon = false
		deferredSingleQuote = false
		value = ""
		valueEndCharacter = ""
		valueFinished = false
	}
	}
	}
	if len(label) > 0 { // ensure we process any line ending abruptly
		data[label] = casifyValue(value, valueType)
	}
	return data
}
