package adaptor

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/adaptor/postgres"
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
	"github.com/compose/transporter/pkg/pipe"

	"database/sql"

	_ "github.com/lib/pq" // import pq driver
)

// Postgres is an adaptor to read / write to postgres.
// it works as a source by copying files, and then optionally tailing the oplog
type Postgres struct {
	// pull these in from the node
	uri             string
	tail            bool   // run the tail oplog
	replicationSlot string // logical replication slot to use for changes
	debug           bool

	// save time by setting these once
	tableMatch *regexp.Regexp
	database   string

	latestLSN string

	//
	pipe *pipe.Pipe
	path string

	// postgres connection and options
	session      *sql.DB
	oplogTimeout time.Duration

	restartable bool // this refers to being able to refresh the iterator, not to the restart based on session op
}

// Config provides configuration options for a postgres adaptor
// the notable difference between this and dbConfig is the presence of the Tail option
type Config struct {
	URI             string `json:"uri" doc:"the uri to connect to, in the form 'user=my-user password=my-password dbname=dbname sslmode=require'"`
	Namespace       string `json:"namespace" doc:"mongo namespace to read/write"`
	Timeout         string `json:"timeout" doc:"timeout for establishing connection, format must be parsable by time.ParseDuration and defaults to 10s"`
	Debug           bool   `json:"debug" doc:"display debug information"`
	Tail            bool   `json:"tail" doc:"if tail is true, then the postgres source will tail the oplog after copying the namespace"`
	ReplicationSlot string `json:"replication_slot" doc:"required if tail is true; sets the replication slot to use for logical decoding"`
	Wc              int    `json:"wc" doc:"The write concern to use for writes, Int, indicating the minimum number of servers to write to before returning success/failure"`
	FSync           bool   `json:"fsync" doc:"When writing, should we flush to disk before returning success"`
	Bulk            bool   `json:"bulk" doc:"use a buffer to bulk insert documents"`
}

func init() {
	adaptor.Add("postgres", adaptor.Creator(func(ppipe *pipe.Pipe, path string, extra adaptor.Config) (adaptor.Adaptor, error) {
		var (
			conf Config
			err  error
		)
		if err = extra.Construct(&conf); err != nil {
			return nil, err
		}

		if conf.URI == "" || conf.Namespace == "" {
			return nil, fmt.Errorf("both uri and namespace required, but missing ")
		}

		if conf.Debug {
			fmt.Printf("Postgres Config %+v\n", conf)
		}

		p := &Postgres{
			restartable:     true,            // assume for that we're able to restart the process
			oplogTimeout:    5 * time.Second, // timeout the oplog iterator
			pipe:            ppipe,
			uri:             conf.URI,
			tail:            conf.Tail,
			replicationSlot: conf.ReplicationSlot,
			debug:           conf.Debug,
			path:            path,
		}

		p.database, p.tableMatch, err = extra.CompileNamespace()
		if err != nil {
			return p, err
		}
		return p, nil
	}))
}

// Description for postgres adaptor
func (p *Postgres) Description() string {
	return "a postgres adaptor that functions as both a source and a sink"
}

const sampleConfig = `
- localpostgres:
    type: postgres
    uri: postgres://127.0.0.1:5432/test
`

// SampleConfig for postgres adaptor
func (p *Postgres) SampleConfig() string {
	return sampleConfig
}

func (p *Postgres) Connect() error {
	var err error
	matchDbName := regexp.MustCompile(fmt.Sprintf("dbname=%v", p.database))
	if match := matchDbName.MatchString(p.uri); !match {
		return fmt.Errorf("Mismatch database name in YAML config and app javascript. Postgres URI should, but does not contain dbname=%v", p.database)
	}
	p.session, err = sql.Open("postgres", p.uri)
	if err != nil {
		return fmt.Errorf("unable to parse uri (%s), %s\n", p.uri, err.Error())
	}
	return nil
}

// Start the adaptor as a source
func (p *Postgres) Start() (err error) {
	defer func() {
		p.pipe.Stop()
	}()

	err = p.catData()
	if err != nil {
		p.pipe.Err <- err
		return fmt.Errorf("Error connecting to Postgres: %v", err)
	}
	if p.tail {
		// listen on logical decoding
		err = p.tailData()
		if err != nil {
			p.pipe.Err <- err
			return err
		}
	}

	return
}

// Listen starts the pipe's listener
func (p *Postgres) Listen() (err error) {
	defer func() {
		p.pipe.Stop()
	}()

	return p.pipe.Listen(p.writeMessage, p.tableMatch)
}

// Stop the adaptor
func (p *Postgres) Stop() error {
	p.pipe.Stop()

	return nil
}

// writeMessage writes one message to the destination Postgres, or sends an error down the pipe
// TODO this can be cleaned up.  I'm not sure whether this should pipe the error, or whether the
//   caller should pipe the error
func (p *Postgres) writeMessage(msg message.Msg) (message.Msg, error) {
	m, err := message.Exec(message.MustUseAdaptor("postgres").(postgres.Adaptor).UseSession(p.session), msg)
	if err != nil {
		p.pipe.Err <- adaptor.NewError(adaptor.ERROR, p.path, fmt.Sprintf("postgres error (%v)", err), msg.Data())
	}

	return m, err
}

// catdata pulls down the original tables
func (p *Postgres) catData() (err error) {
	fmt.Println("Exporting data from matching tables:")
	tablesResult, err := p.session.Query("SELECT table_schema,table_name FROM information_schema.tables")
	if err != nil {
		return err
	}
	for tablesResult.Next() {
		var schema string
		var tname string
		err = tablesResult.Scan(&schema, &tname)

		_, err := p.catTable(schema, tname, p.pipe)
		if err != nil {
			return err
		}
	}
	return
}

func (p *Postgres) catTable(tableSchema string, tableName string, outputInterface interface{}) (rowsSlice []message.Msg, err error) {
	// determine if table should be copied
	schemaAndTable := fmt.Sprintf("%v.%v", tableSchema, tableName)
	if strings.HasPrefix(schemaAndTable, "information_schema.") || strings.HasPrefix(schemaAndTable, "pg_catalog.") {
		return
	} else if match := p.tableMatch.MatchString(schemaAndTable); !match {
		return
	}

	fmt.Printf("  exporting %v.%v\n", tableSchema, tableName)

	// get columns for table
	columnsResult, err := p.session.Query(fmt.Sprintf(`
SELECT c.column_name, c.data_type, e.data_type AS element_type
FROM information_schema.columns c LEFT JOIN information_schema.element_types e
     ON ((c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier)
       = (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier))
WHERE c.table_schema = '%v' AND c.table_name = '%v'
ORDER BY c.ordinal_position;
`, tableSchema, tableName))
	if err != nil {
		return rowsSlice, err
	}
	var columns [][]string
	for columnsResult.Next() {
		var columnName string
		var columnType string
		var columnArrayType sql.NullString // this value may be nil

		err = columnsResult.Scan(&columnName, &columnType, &columnArrayType)
		recoveredRegex := regexp.MustCompile("recovered")
		if err != nil && !recoveredRegex.MatchString(err.Error()) {
			return rowsSlice, err
		}

		if columnType == "ARRAY" {
			columnType = fmt.Sprintf("%v[]", columnArrayType.String) // append [] to columnType if array
		}

		column := []string{columnName, columnType}
		columns = append(columns, column)
	}

	// build docs for table
	docsResult, err := p.session.Query(fmt.Sprintf("SELECT * FROM %v", schemaAndTable))
	if err != nil {
		return rowsSlice, err
	}

	// Set output type to pipe or slice
	var (
		outputPipe   *pipe.Pipe
		outputToPipe bool
	)
	switch outputInterface.(type) {
	case *pipe.Pipe:
		outputToPipe = true
		outputPipe = outputInterface.(*pipe.Pipe)
	default:
		outputToPipe = false
		rowsSlice = make([]message.Msg, 0)
	}

	for docsResult.Next() {
		dest := make([]interface{}, len(columns))
		for i := range columns {
			dest[i] = make([]byte, 30)
			dest[i] = &dest[i]
		}

		var docMap map[string]interface{}
		err = docsResult.Scan(dest...)
		if err != nil {
			fmt.Println("Failed to scan row", err)
			return rowsSlice, err
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
		msg := message.MustUseAdaptor("postgres").From(ops.Insert, schemaAndTable, data.Data(docMap))
		if outputToPipe {
			outputPipe.Send(msg)
		} else {
			rowsSlice = append(rowsSlice, msg)
		}
	}

	return rowsSlice, err
}

// tail the logical data
func (p *Postgres) tailData() (err error) {
	fmt.Printf("Listening for changes on logical decoding slot '%v'\n", p.replicationSlot)
	for {
		msgSlice, err := p.pluckFromLogicalDecoding()
		if err != nil {
			return err
		}
		for _, msg := range msgSlice {
			p.pipe.Send(msg)
		}
		time.Sleep(3 * time.Second)
	}
}

// Use Postgres logical decoding to retreive the latest changes
func (p *Postgres) pluckFromLogicalDecoding() ([]message.Msg, error) {
	var result []message.Msg
	dataMatcher := regexp.MustCompile("^table ([^\\.]+).([^\\.]+): (INSERT|DELETE|UPDATE): (.+)$") // 1 - schema, 2 - table, 3 - action, 4 - remaining

	changesResult, err := p.session.Query(fmt.Sprintf("SELECT * FROM pg_logical_slot_get_changes('%v', NULL, NULL);", p.replicationSlot))
	if err != nil {
		p.pipe.Err <- err
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

		// Skippable because no pimary key on record
		// Make sure we are getting changes on valid tables
		schemaAndTable := fmt.Sprintf("%v.%v", dataMatches[1], dataMatches[2])
		if match := p.tableMatch.MatchString(schemaAndTable); !match {
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

		docMap, err := p.parseLogicalDecodingData(dataMatches[4])
		if err != nil {
			return result, err
		}
		msg := message.MustUseAdaptor("postgres").From(action, schemaAndTable, data.Data(docMap))
		result = append(result, msg)
	}

	return result, err
}

func (p *Postgres) parseLogicalDecodingData(data string) (docMap map[string]interface{}, err error) {
	docMap = make(map[string]interface{})
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

	for _, character := range data {
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
		docMap[label] = casifyValue(value, valueType)

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
		docMap[label] = casifyValue(value, valueType)
	}
	return
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
