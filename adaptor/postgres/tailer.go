package postgres

import (
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
	_ client.Reader = &Tailer{}
)

// Tailer implements the behavior defined by client.Tailer for interfacing with the MongoDB oplog.
type Tailer struct {
	reader          client.Reader
	db              string
	replicationSlot string
}

func newTailer(db, replicationSlot string) client.Reader {
	return &Tailer{newReader(db), db, replicationSlot}
}

// Tail does the things
func (t *Tailer) Read(filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan message.Msg, error) {
		readFunc := t.reader.Read(filterFn)
		msgChan, err := readFunc(s, done)
		if err != nil {
			return nil, err
		}
		out := make(chan message.Msg)
		go func() {
			defer close(out)
			// read until reader done
			for msg := range msgChan {
				out <- msg
			}

			// start tailing
			log.With("db", t.db).With("logical_decoding_slot", t.replicationSlot).Infoln("Listening for changes...")
			for {
				select {
				case <-done:
					log.With("db", t.db).Infoln("tailing stopping...")
					return
				case <-time.After(time.Second):
					msgSlice, err := t.pluckFromLogicalDecoding(s.(*Session), filterFn)
					if err != nil {
						log.With("db", t.db).Errorf("error plucking from logical decoding %v", err)
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

// Use Postgres logical decoding to retreive the latest changes
func (t *Tailer) pluckFromLogicalDecoding(s *Session, filterFn client.NsFilterFunc) ([]message.Msg, error) {
	var result []message.Msg
	dataMatcher := regexp.MustCompile("^table ([^\\.]+).([^\\.]+): (INSERT|DELETE|UPDATE): (.+)$") // 1 - schema, 2 - table, 3 - action, 4 - remaining

	changesResult, err := s.pqSession.Query(fmt.Sprintf("SELECT * FROM pg_logical_slot_get_changes('%v', NULL, NULL);", t.replicationSlot))
	if err != nil {
		return result, err
	}

	for changesResult.Next() {
		var (
			location string
			xid      string
			d        string
		)

		err = changesResult.Scan(&location, &xid, &d)
		if err != nil {
			return result, err
		}

		// Ensure we are getting a data change row
		dataMatches := dataMatcher.FindStringSubmatch(d)
		if len(dataMatches) == 0 {
			continue
		}

		// Skippable because no primary key on record
		// Make sure we are getting changes on valid tables
		schemaAndTable := fmt.Sprintf("%v.%v", dataMatches[1], dataMatches[2])
		if !filterFn(schemaAndTable) {
			continue
		}

		if dataMatches[4] == "(no-tuple-data)" {
			fmt.Printf("No tuple data for action %v on %v.%v\n", dataMatches[3], dataMatches[1], dataMatches[2])
			continue
		}

		// normalize the action
		var action ops.Op
		switch {
		case dataMatches[3] == "INSERT":
			action = ops.Insert
		case dataMatches[3] == "DELETE":
			action = ops.Delete
		case dataMatches[3] == "UPDATE":
			action = ops.Update
		case true:
			return result, fmt.Errorf("Error processing action from string: %v", d)
		}

		fmt.Printf("Received %v from Postgres on %v.%v\n", dataMatches[3], dataMatches[1], dataMatches[2])

		docMap := parseLogicalDecodingData(dataMatches[4])
		msg := message.From(action, schemaAndTable, docMap)
		result = append(result, msg)
	}

	return result, err
}

func parseLogicalDecodingData(d string) data.Data {
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
			value = value[1:len(value)]
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

		//default:
		//fmt.Printf("Could not transform type '%v' with value '%v'\n", valueType, value)
	}

	return value
}
