package mysql

import (
	"context"
	"fmt"
	"regexp"
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

// Copy Canal? E.g: https://github.com/go-mysql-org/go-mysql/blob/d1666538b005e996414063695ca223994e9dc19d/canal/sync.go#L38
// Also see: https://github.com/go-mysql-org/go-mysql/blob/master/replication/event.go
// And: https://github.com/go-mysql-org/go-mysql/blob/d1666538b005e996414063695ca223994e9dc19d/replication/binlogsyncer.go#L750
//
// In Postgresql this returns multiple events. For MySQL it will just be one
// This _might_ end up being merged with parseBinlogData as a result
func (t *Tailer) processEvent(event *replication.BinlogEvent, filterFn client.NsFilterFunc) (client.MessageSet, bool, error) {
	var (
		result client.MessageSet
		skip = false
		err error
		// For now just to get things to build whilst concentrating on the Read function
		d = "blah"
	)
	
	// This is specific to Postgresql:
	// https://www.postgresql.org/docs/9.4/logicaldecoding-example.html
	dataMatcher := regexp.MustCompile(`(?s)^table ([^\.]+)\.([^:]+): (INSERT|DELETE|UPDATE): (.+)$`) // 1 - schema, 2 - table, 3 - action, 4 - remaining

	// Something like this? Or does it make more sense to combine parseEvent and processEvent?
	// Or should parseEvent by specific to "4 - remaining" up above?
	//d, _ := parseEvent(event)

	// Ensure we are getting a data change row
	dataMatches := dataMatcher.FindStringSubmatch(d)
	if len(dataMatches) == 0 {
		skip = true
	}
	// Skippable because no primary key on record
	// Make sure we are getting changes on valid tables
	schemaAndTable := fmt.Sprintf("%v.%v", dataMatches[1], dataMatches[2])
	if !filterFn(schemaAndTable) {
		skip = true
	}
	if dataMatches[4] == "(no-tuple-data)" {
		log.With("op", dataMatches[3]).With("schema", schemaAndTable).Infoln("no tuple data")
		skip = true
	}
	// normalize the action
	var action ops.Op
	switch dataMatches[3] {
	case "INSERT":
		action = ops.Insert
	case "DELETE":
		action = ops.Delete
	case "UPDATE":
		action = ops.Update
	default:
		return result, skip, fmt.Errorf("Error processing action from string: %v", d)
	}
	log.With("op", action).With("table", schemaAndTable).Debugln("received")
	docMap := parseEvent(dataMatches[4])
	result = client.MessageSet{
		Msg:  message.From(action, schemaAndTable, docMap),
		Mode: commitlog.Sync,
	}

	return result, skip, err
}

func parseEvent(d string) data.Data {
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

	for _, character := range d {
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
	if len(label) > 0 { // ensure we process any line ending abruptly
		data[label] = casifyValue(value, valueType)
	}
	return data
}
