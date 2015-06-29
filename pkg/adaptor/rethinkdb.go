package adaptor

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
	version "github.com/hashicorp/go-version"
	gorethink "gopkg.in/dancannon/gorethink.v0"
)

// Rethinkdb is an adaptor that writes metrics to rethinkdb (http://rethinkdb.com/)
// An open-source distributed database
type Rethinkdb struct {
	// pull these in from the config
	uri *url.URL

	// save time by setting these once
	database string
	table    string

	debug bool
	tail  bool

	//
	pipe *pipe.Pipe
	path string

	// rethinkdb connection and options
	client *gorethink.Session
}

// rethinkDbConfig provides custom configuration options for the RethinkDB adapter
type rethinkDbConfig struct {
	URI       string `json:"uri" doc:"the uri to connect to, in the form rethink://user:password@host.example:28015/database"`
	Namespace string `json:"namespace" doc:"rethink namespace to read/write, in the form database.table"`
	Debug     bool   `json:"debug" doc:"if true, verbose debugging information is displayed"`
	Tail      bool   `json:"tail" doc:"if true, the RethinkDB table will be monitored for changes after copying the namespace"`
	Timeout   int    `json:"timeout" doc:"timeout, in seconds, for connect, read, and write operations to the RethinkDB server; default is 10"`
}

type rethinkDbChangeNotification struct {
	Error  string                 `gorethink:"error"`
	OldVal map[string]interface{} `gorethink:"old_val"`
	NewVal map[string]interface{} `gorethink:"new_val"`
}

type rethinkDbProcessStatus struct {
	Version string `gorethink:"version"`
}

type rethinkDbServerStatus struct {
	Process rethinkDbProcessStatus `gorethink:"process"`
}

var (
	rethinkDbVersionMatcher *regexp.Regexp = regexp.MustCompile(`\d+\.\d+(\.\d+)?`)
)

// NewRethinkdb creates a new Rethinkdb database adaptor
func NewRethinkdb(p *pipe.Pipe, path string, extra Config) (StopStartListener, error) {
	var (
		conf rethinkDbConfig
		err  error
	)
	if err = extra.Construct(&conf); err != nil {
		return nil, err
	}

	u, err := url.Parse(conf.URI)
	if err != nil {
		return nil, err
	}

	if conf.Debug {
		fmt.Printf("rethinkDbConfig: %#v\n", conf)
	}

	r := &Rethinkdb{
		uri:  u,
		pipe: p,
		path: path,
		tail: conf.Tail,
	}

	r.database, r.table, err = extra.splitNamespace()
	if err != nil {
		return r, err
	}
	r.debug = conf.Debug

	opts := gorethink.ConnectOpts{
		Address: r.uri.Host,
		MaxIdle: 10,
		Timeout: time.Second * 10,
	}
	if conf.Timeout > 0 {
		opts.Timeout = time.Duration(conf.Timeout) * time.Second
	}

	r.client, err = gorethink.Connect(opts)
	if err != nil {
		return r, err
	}
	r.client.Use(r.database)

	if r.tail {
		constraint, _ := version.NewConstraint(">= 1.16")
		if err := r.assertServerVersion(constraint); err != nil {
			return r, err
		}
	}

	return r, nil
}

func (r *Rethinkdb) assertServerVersion(constraint version.Constraints) error {
	cursor, err := gorethink.Db("rethinkdb").Table("server_status").Run(r.client)
	if err != nil {
		return err
	}

	if cursor.IsNil() {
		return errors.New("could not determine the RethinkDB server version: no rows returned from the server_status table")
	}

	var serverStatus rethinkDbServerStatus
	cursor.Next(&serverStatus)

	if serverStatus.Process.Version == "" {
		return errors.New("could not determine the RethinkDB server version: process.version key missing")
	}

	pieces := strings.Split(serverStatus.Process.Version, " ")
	if len(pieces) < 2 {
		return fmt.Errorf("could not determine the RethinkDB server version: malformed version string (%v)", serverStatus.Process.Version)
	}

	versionString := rethinkDbVersionMatcher.FindString(pieces[1])
	if versionString == "" {
		return fmt.Errorf("could not determine the RethinkDB server version: malformed version string (%v)", serverStatus.Process.Version)
	}

	if r.debug {
		fmt.Printf("RethinkDB version: %v\n", versionString)
	}

	serverVersion, err := version.NewVersion(versionString)
	if err != nil {
		return fmt.Errorf("could not determine the RethinkDB server version: malformed version string (%v)", serverStatus.Process.Version)
	}

	if !constraint.Check(serverVersion) {
		return fmt.Errorf("RethinkDB server version too old: expected %v, but was %v", constraint, serverVersion)
	}

	return nil
}

// Start the adaptor as a source
func (r *Rethinkdb) Start() error {
	if r.debug {
		fmt.Printf("getting a changes cursor\n")
	}

	// Grab a changes cursor before sending all rows. The server will buffer
	// changes while we reindex the entire table.
	var ccursor *gorethink.Cursor
	ccursor, err := gorethink.Table(r.table).Changes().Run(r.client)
	if err != nil {
		r.pipe.Err <- err
		return err
	}
	defer ccursor.Close()

	if err := r.sendAllDocuments(); err != nil {
		r.pipe.Err <- err
		return err
	}

	if r.tail {
		if err := r.sendChanges(ccursor); err != nil {
			r.pipe.Err <- err
			return err
		}
	}

	return nil
}

func (r *Rethinkdb) sendAllDocuments() error {
	if r.debug {
		fmt.Printf("sending all documents\n")
	}

	cursor, err := gorethink.Table(r.table).Run(r.client)
	if err != nil {
		return err
	}
	defer cursor.Close()

	var doc map[string]interface{}
	for cursor.Next(&doc) {
		if stop := r.pipe.Stopped; stop {
			return nil
		}

		msg := message.NewMsg(message.Insert, r.prepareDocument(doc))
		r.pipe.Send(msg)
	}

	if err := cursor.Err(); err != nil {
		return err
	}

	return nil
}

func (r *Rethinkdb) sendChanges(ccursor *gorethink.Cursor) error {
	if r.debug {
		fmt.Printf("sending changes\n")
	}

	var change rethinkDbChangeNotification
	for ccursor.Next(&change) {
		if stop := r.pipe.Stopped; stop {
			return nil
		}

		if r.debug {
			fmt.Printf("change: %#v\n", change)
		}

		var msg *message.Msg
		if change.Error != "" {
			return errors.New(change.Error)
		} else if change.OldVal != nil && change.NewVal != nil {
			msg = message.NewMsg(message.Update, r.prepareDocument(change.NewVal))
		} else if change.NewVal != nil {
			msg = message.NewMsg(message.Insert, r.prepareDocument(change.NewVal))
		} else if change.OldVal != nil {
			msg = message.NewMsg(message.Delete, r.prepareDocument(change.OldVal))
		}

		if msg != nil {
			fmt.Printf("msg: %#v\n", msg)
			r.pipe.Send(msg)
		}
	}

	if err := ccursor.Err(); err != nil {
		return err
	}

	return nil
}

// prepareDocument moves the `id` field to the `_id` field, which is more
// commonly used by downstream sinks. A transformer could be used to do the
// same thing, but because transformers are not run for Delete messages, we
// must do it here.
func (r *Rethinkdb) prepareDocument(doc map[string]interface{}) map[string]interface{} {
	doc["_id"] = doc["id"]
	delete(doc, "id")

	return doc
}

// Listen start's the adaptor's listener
func (r *Rethinkdb) Listen() (err error) {
	r.recreateTable()
	return r.pipe.Listen(r.applyOp)
}

// Stop the adaptor
func (r *Rethinkdb) Stop() error {
	r.pipe.Stop()
	return nil
}

// applyOp applies one operation to the database
func (r *Rethinkdb) applyOp(msg *message.Msg) (*message.Msg, error) {
	var (
		resp gorethink.WriteResponse
		err  error
	)

	if !msg.IsMap() {
		r.pipe.Err <- NewError(ERROR, r.path, "rethinkdb error (document must be a json document)", msg.Data)
		return msg, nil
	}
	doc := msg.Map()

	switch msg.Op {
	case message.Delete:
		id, err := msg.IDString("id")
		if err != nil {
			r.pipe.Err <- NewError(ERROR, r.path, "rethinkdb error (cannot delete an object with a nil id)", msg.Data)
			return msg, nil
		}
		resp, err = gorethink.Table(r.table).Get(id).Delete().RunWrite(r.client)
	case message.Insert:
		resp, err = gorethink.Table(r.table).Insert(doc).RunWrite(r.client)
	case message.Update:
		resp, err = gorethink.Table(r.table).Insert(doc, gorethink.InsertOpts{Conflict: "replace"}).RunWrite(r.client)
	}
	if err != nil {
		r.pipe.Err <- NewError(ERROR, r.path, "rethinkdb error (%s)", err)
		return msg, nil
	}

	err = r.handleResponse(&resp)
	if err != nil {
		r.pipe.Err <- NewError(ERROR, r.path, "rethinkdb error (%s)", err)
	}

	return msg, nil
}

func (r *Rethinkdb) recreateTable() {
	if r.debug {
		fmt.Printf("dropping and creating table '%s' on database '%s'\n", r.table, r.database)
	}
	gorethink.Db(r.database).TableDrop(r.table).RunWrite(r.client)
	gorethink.Db(r.database).TableCreate(r.table).RunWrite(r.client)
}

// handleresponse takes the rethink response and turn it into something we can consume elsewhere
func (r *Rethinkdb) handleResponse(resp *gorethink.WriteResponse) error {
	if resp.Errors != 0 {
		if !strings.Contains(resp.FirstError, "Duplicate primary key") { // we don't care about this error
			if r.debug {
				fmt.Printf("Reported %d errors\n", resp.Errors)
			}
			return fmt.Errorf("%s\n%s", "problem inserting docs", resp.FirstError)
		}
	}
	return nil
}
