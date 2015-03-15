package adaptor

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
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

// RethinkdbConfig provides custom configuration options for the RethinkDB adapter
type RethinkdbConfig struct {
	URI       string `json:"uri" doc:"the uri to connect to, in the form rethink://user:password@host.example:28015/database"`
	Namespace string `json:"namespace" doc:"rethink namespace to read/write, in the form database.table"`
	Debug     bool   `json:"debug" doc:"if true, verbose debugging information is displayed"`
	Tail      bool   `json:"tail" doc:"if true, the RethinkDB table will be monitored for changes after copying the namespace"`
}

type rethinkDbChangeNotification map[string]map[string]interface{}

// NewRethinkdb creates a new Rethinkdb database adaptor
func NewRethinkdb(p *pipe.Pipe, path string, extra Config) (StopStartListener, error) {
	var (
		conf RethinkdbConfig
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
		fmt.Printf("RethinkdbConfig: %#v\n", conf)
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

	r.client, err = gorethink.Connect(gorethink.ConnectOpts{
		Address: r.uri.Host,
		MaxIdle: 10,
		Timeout: time.Second * 10,
	})
	if err != nil {
		return nil, err
	}
	r.client.Use(r.database)

	return r, nil
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

	if err := r.sendAllDocuments(); err != nil {
		r.pipe.Err <- err
		return err
	}

	// Monitor for changes
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

	var doc map[string]interface{}
	for cursor.Next(&doc) {
		if stop := r.pipe.Stopped; stop {
			return nil
		}

		msg := message.NewMsg(message.Insert, doc)
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

	// { "new_val": {...}, "old_val": {...} }
	var change rethinkDbChangeNotification
	for ccursor.Next(&change) {
		if stop := r.pipe.Stopped; stop {
			return nil
		}

		var msg *message.Msg
		if change["old_val"] != nil && change["new_val"] != nil {
			msg = message.NewMsg(message.Update, change["new_val"])
		} else if change["new_val"] != nil {
			msg = message.NewMsg(message.Insert, change["new_val"])
		} else if change["old_val"] != nil {
			msg = message.NewMsg(message.Delete, change["old_val"])
		}

		if msg != nil {
			r.pipe.Send(msg)
		}
	}

	if err := ccursor.Err(); err != nil {
		return err
	}

	return nil
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
