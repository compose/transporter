package adaptor

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"

	"database/sql"
	_ "github.com/lib/pq"
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
	postgresSession *sql.DB
	oplogTimeout    time.Duration

	restartable bool // this refers to being able to refresh the iterator, not to the restart based on session op
}

// PostgresConfig provides configuration options for a postgres adaptor
// the notable difference between this and dbConfig is the presence of the Tail option
type PostgresConfig struct {
	URI             string `json:"uri" doc:"the uri to connect to, in the form 'user=my-user password=my-password dbname=dbname sslmode=require'"`
	Namespace       string `json:"namespace" doc:"mongo namespace to read/write"`
	Timeout         string `json:timeout" doc:"timeout for establishing connection, format must be parsable by time.ParseDuration and defaults to 10s"`
	Debug           bool   `json:"debug" doc:"display debug information"`
	Tail            bool   `json:"tail" doc:"if tail is true, then the postgres source will tail the oplog after copying the namespace"`
	ReplicationSlot string `json:"replication_slot" doc:"required if tail is true; sets the replication slot to use for logical decoding"`
	Wc              int    `json:"wc" doc:"The write concern to use for writes, Int, indicating the minimum number of servers to write to before returning success/failure"`
	FSync           bool   `json:"fsync" doc:"When writing, should we flush to disk before returning success"`
	Bulk            bool   `json:"bulk" doc:"use a buffer to bulk insert documents"`
}

// NewPostgres creates a new Postgres adaptor
func NewPostgres(p *pipe.Pipe, path string, extra Config) (StopStartListener, error) {
	var (
		conf PostgresConfig
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

	postgres := &Postgres{
		restartable:     true,            // assume for that we're able to restart the process
		oplogTimeout:    5 * time.Second, // timeout the oplog iterator
		pipe:            p,
		uri:             conf.URI,
		tail:            conf.Tail,
		replicationSlot: conf.ReplicationSlot,
		debug:           conf.Debug,
		path:            path,
	}

	postgres.database, postgres.tableMatch, err = extra.compileNamespace()
	if err != nil {
		return postgres, err
	}

	matchDbName := regexp.MustCompile(fmt.Sprintf("dbname=%v", postgres.database))
	if match := matchDbName.MatchString(postgres.uri); !match {
		return postgres, fmt.Errorf("Mismatch database name in YAML config and app javascript.  Postgres URI should, but does not contain dbname=%v", postgres.database)
	}

	postgres.postgresSession, err = sql.Open("postgres", postgres.uri)
	if err != nil {
		return postgres, fmt.Errorf("unable to parse uri (%s), %s\n", postgres.uri, err.Error())
	}

	return postgres, nil
}

// Start the adaptor as a source
func (postgres *Postgres) Start() (err error) {
	defer func() {
		postgres.pipe.Stop()
	}()

	err = postgres.catData()
	if err != nil {
		postgres.pipe.Err <- err
		return fmt.Errorf("Error connecting to Postgres: %v", err)
	}
	if postgres.tail {
		// listen on logical decoding
		err = postgres.tailData()
		if err != nil {
			postgres.pipe.Err <- err
			return err
		}
	}

	return
}

// Listen starts the pipe's listener
func (postgres *Postgres) Listen() (err error) {
	defer func() {
		postgres.pipe.Stop()
	}()

	return postgres.pipe.Listen(postgres.writeMessage, postgres.tableMatch)
}

// Stop the adaptor
func (postgres *Postgres) Stop() error {
	postgres.pipe.Stop()

	return nil
}

// writeMessage writes one message to the destination Postgres, or sends an error down the pipe
// TODO this can be cleaned up.  I'm not sure whether this should pipe the error, or whether the
//   caller should pipe the error
func (postgres *Postgres) writeMessage(msg *message.Msg) (*message.Msg, error) {
	switch {
	case msg.Op == message.Insert:
		fmt.Printf("Write INSERT to Postgres %v\n", msg.Namespace)
		var (
			keys         []string
			placeholders []string
			data         []interface{}
		)

		i := 1
		for key, value := range msg.Map() {
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

		query := fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v);", msg.Namespace, strings.Join(keys, ", "), strings.Join(placeholders, ", "))
		_, err := postgres.postgresSession.Exec(query, data...)

		if err != nil {
			fmt.Printf("Error INSERTING to Postgres with error (%v) on query (%v) with data (%v)\n", err, query, data)
			return msg, nil
		}
	case msg.Op == message.Update:
		fmt.Printf("Write UPDATE to Postgres %v\n", msg.Namespace)
		var (
			conditional_keys []string
			update_keys      []string
			values           []interface{}
		)

		primary_keys, err := postgres.primaryKeys(msg.Namespace)
		if err != nil {
			return msg, err
		}

		i := 1
		for key, value := range msg.Map() {
			if primary_keys[key] { // key is primary key
				conditional_keys = append(conditional_keys, fmt.Sprintf("%v=$%v", key, i))
			} else {
				update_keys = append(update_keys, fmt.Sprintf("%v=$%v", key, i))
			}

			switch value.(type) {
			case map[string]interface{}:
				value, _ = json.Marshal(value)
			case []interface{}:
				value, _ = json.Marshal(value)
				value = string(value.([]byte))
				value = fmt.Sprintf("{%v}", value.(string)[1:len(value.(string))-1])
			}
			values = append(values, value)

			i = i + 1
		}

		if len(primary_keys) != len(conditional_keys) {
			return msg, fmt.Errorf("All primary keys were not accounted for.  Provided: %v; Required; %v", conditional_keys, primary_keys)
		}

		query := fmt.Sprintf("UPDATE %v SET %v WHERE %v;", msg.Namespace, strings.Join(update_keys, ", "), strings.Join(conditional_keys, " AND "))
		_, err = postgres.postgresSession.Exec(query, values...)

		if err != nil {
			return msg, fmt.Errorf("Error UPDATING to Postgres with error (%v) on query (%v) with values (%v)\n", err, query, values)
		}
	case msg.Op == message.Delete:
		fmt.Printf("Write DELETE to Postgres %v values %v\n", msg.Namespace, msg.Data)
		var (
			conditional_keys []string
			values           []interface{}
		)

		primary_keys, err := postgres.primaryKeys(msg.Namespace)
		if err != nil {
			return msg, err
		}

		i := 1
		for key, value := range msg.Map() {
			if primary_keys[key] { // key is primary key
				conditional_keys = append(conditional_keys, fmt.Sprintf("%v = $%v", key, i))
			}

			switch value.(type) {
			case map[string]interface{}:
				value, _ = json.Marshal(value)
			}
			values = append(values, value)

			i = i + 1
		}

		if len(primary_keys) != len(conditional_keys) {
			return msg, fmt.Errorf("All primary keys were not accounted for.  Provided: %v; Required; %v", conditional_keys, primary_keys)
		}

		query := fmt.Sprintf("DELETE FROM %v WHERE %v;", msg.Namespace, strings.Join(conditional_keys, " AND "))
		_, err = postgres.postgresSession.Exec(query, values...)
		if err != nil {
			return msg, fmt.Errorf("Error DELETEING from Postgres with error (%v) on query (%v) with values (%v)\n", err, query, values)
		}
	}

	return msg, nil
}

// catdata pulls down the original tables
func (postgres *Postgres) catData() (err error) {
	fmt.Println("Exporting data from matching tables:")
	tablesResult, err := postgres.postgresSession.Query("SELECT table_schema,table_name FROM information_schema.tables")
	if err != nil {
		return err
	}
	for tablesResult.Next() {
		var table_schema string
		var table_name string
		err = tablesResult.Scan(&table_schema, &table_name)

		_, err := postgres.catTable(table_schema, table_name, postgres.pipe)
		if err != nil {
			return err
		}
	}
	return
}

func (postgres *Postgres) catTable(table_schema string, table_name string, outputInterface interface{}) (rowsSlice []*message.Msg, err error) {
	// determine if table should be copied
	schemaAndTable := fmt.Sprintf("%v.%v", table_schema, table_name)
	if strings.HasPrefix(schemaAndTable, "information_schema.") || strings.HasPrefix(schemaAndTable, "pg_catalog.") {
		return
	} else if match := postgres.tableMatch.MatchString(schemaAndTable); !match {
		return
	}

	fmt.Printf("  exporting %v.%v\n", table_schema, table_name)

	// get columns for table
	columnsResult, err := postgres.postgresSession.Query(fmt.Sprintf(`
SELECT c.column_name, c.data_type, e.data_type AS element_type
FROM information_schema.columns c LEFT JOIN information_schema.element_types e
     ON ((c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier)
       = (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier))
WHERE c.table_schema = '%v' AND c.table_name = '%v'
ORDER BY c.ordinal_position;
`, table_schema, table_name))
	if err != nil {
		return rowsSlice, err
	}
	var columns [][]string
	for columnsResult.Next() {
		var columnName string
		var columnType string
		var columnArrayType sql.NullString // this value may be nil

		err := columnsResult.Scan(&columnName, &columnType, &columnArrayType)
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
	docsResult, err := postgres.postgresSession.Query(fmt.Sprintf("SELECT * FROM %v", schemaAndTable))
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
		rowsSlice = make([]*message.Msg, 0)
	}

	for docsResult.Next() {
		dest := make([]interface{}, len(columns))
		for i, _ := range columns {
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

		msg := message.NewMsg(message.Insert, docMap, schemaAndTable)
		if outputToPipe {
			outputPipe.Send(msg)
		} else {
			rowsSlice = append(rowsSlice, msg)
		}
	}

	return rowsSlice, err
}

// tail the logical data
func (postgres *Postgres) tailData() (err error) {
	fmt.Printf("Listening for changes on logical decoding slot '%v'\n", postgres.replicationSlot)
	for {
		msgSlice, err := postgres.pluckFromLogicalDecoding()
		if err != nil {
			return err
		}
		for _, msg := range msgSlice {
			postgres.pipe.Send(msg)
		}
		time.Sleep(3 * time.Second)
	}
	return
}

// Use Postgres logical decoding to retreive the latest changes
func (postgres *Postgres) pluckFromLogicalDecoding() ([]*message.Msg, error) {
	result := make([]*message.Msg, 0)
	dataMatcher := regexp.MustCompile("^table ([^\\.]+).([^\\.]+): (INSERT|DELETE|UPDATE): (.+)$") // 1 - schema, 2 - table, 3 - action, 4 - remaining

	changesResult, err := postgres.postgresSession.Query(fmt.Sprintf("SELECT * FROM pg_logical_slot_get_changes('%v', NULL, NULL);", postgres.replicationSlot))
	if err != nil {
		postgres.pipe.Err <- err
		return result, err
	}

	for changesResult.Next() {
		var (
			location string
			xid      string
			data     string
		)

		err = changesResult.Scan(&location, &xid, &data)
		if err != nil {
			return result, err
		}

		// Ensure we are getting a data change row
		dataMatches := dataMatcher.FindStringSubmatch(data)
		if len(dataMatches) == 0 {
			continue
		}

		// Skippable because no pimary key on record
		// Make sure we are getting changes on valid tables
		schemaAndTable := fmt.Sprintf("%v.%v", dataMatches[1], dataMatches[2])
		if match := postgres.tableMatch.MatchString(schemaAndTable); !match {
			continue
		}

		if dataMatches[4] == "(no-tuple-data)" {
			fmt.Printf("No tuple data for action %v on %v.%v\n", dataMatches[3], dataMatches[1], dataMatches[2])
			continue
		}

		// normalize the action
		var action message.OpType
		switch {
		case dataMatches[3] == "INSERT":
			action = message.Insert
		case dataMatches[3] == "DELETE":
			action = message.Delete
		case dataMatches[3] == "UPDATE":
			action = message.Update
		case true:
			return result, fmt.Errorf("Error processing action from string: %v", data)
		}

		fmt.Printf("Received %v from Postgres on %v.%v\n", dataMatches[3], dataMatches[1], dataMatches[2])

		docMap, err := postgres.ParseLogicalDecodingData(dataMatches[4])
		if err != nil {
			return result, err
		}

		msg := message.NewMsg(action, docMap, schemaAndTable)
		result = append(result, msg)
	}

	return result, err
}

func (postgres *Postgres) ParseLogicalDecodingData(data string) (docMap map[string]interface{}, err error) {
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
		defferedSingleQuote    bool
		valueFinished          bool
	)

	valueTypeFinished = false
	labelFinished = false
	skippedColon = false
	defferedSingleQuote = false
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
		if defferedSingleQuote && string(character) == " " { // we hit an unescaped single quote
			valueFinished = true
		} else if defferedSingleQuote && string(character) == "'" { // we hit an escaped single quote ''
			defferedSingleQuote = false
		} else if string(character) == "'" && !defferedSingleQuote { // we hit a first single quote
			defferedSingleQuote = true
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
		defferedSingleQuote = false
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
		result := make([]interface{}, 0)
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

func (postgres *Postgres) primaryKeys(namespace string) (primaryKeys map[string]bool, err error) {
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

	tablesResult, err := postgres.postgresSession.Query(fmt.Sprintf(`
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
