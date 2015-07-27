package adaptor

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
	version "github.com/hashicorp/go-version"
	gorethink "gopkg.in/dancannon/gorethink.v1"
)

// Rethinkdb is an adaptor that writes metrics to rethinkdb (http://rethinkdb.com/)
// An open-source distributed database
type Rethinkdb struct {
	// pull these in from the config
	uri *url.URL

	// save time by setting these once
	database   string
	tableMatch *regexp.Regexp

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
	Namespace string `json:"namespace" doc:"rethink namespace to read/write"`
	Debug     bool   `json:"debug" doc:"if true, verbose debugging information is displayed"`
	Tail      bool   `json:"tail" doc:"if true, the RethinkDB table will be monitored for changes after copying the namespace"`
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
	rethinkDbVersionMatcher = regexp.MustCompile(`\d+\.\d+(\.\d+)?`)
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
		fmt.Printf("RethinkDB Config %+v\n", conf)
	}

	r := &Rethinkdb{
		uri:  u,
		pipe: p,
		path: path,
		tail: conf.Tail,
	}

	r.database, r.tableMatch, err = extra.compileNamespace()
	if err != nil {
		return r, err
	}
	r.debug = conf.Debug
	if r.debug {
		fmt.Printf("tableMatch: %+v\n", r.tableMatch)
	}

	// test the connection with a timeout
	testConn, err := gorethink.Connect(gorethink.ConnectOpts{
		Address: r.uri.Host,
		Timeout: time.Second * 10,
	})
	if err != nil {
		return r, err
	}
	testConn.Close()

	// we don't want a timeout here because we want to keep connections open
	r.client, err = gorethink.Connect(gorethink.ConnectOpts{
		Address: r.uri.Host,
		MaxIdle: 10,
	})
	if err != nil {
		return r, err
	}
	r.client.Use(r.database)

	constraint, _ := version.NewConstraint(">= 2.0")
	if err := r.assertServerVersion(constraint); err != nil {
		return r, err
	}

	return r, nil
}

func (r *Rethinkdb) assertServerVersion(constraint version.Constraints) error {
	cursor, err := gorethink.DB("rethinkdb").Table("server_status").Run(r.client)
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
	tables, err := gorethink.DB(r.database).TableList().Run(r.client)
	if err != nil {
		return err
	}
	defer tables.Close()

	var (
		wg     sync.WaitGroup
		outerr error
		table  string
	)
	for tables.Next(&table) {
		if match := r.tableMatch.MatchString(table); !match {
			if r.debug {
				fmt.Printf("table, %s, didn't match\n", table)
			}
			continue
		}
		wg.Add(1)
		start := make(chan bool)
		if r.tail {
			go r.startupChangesForTable(table, start, &wg)
		}
		go func(table string) {
			defer wg.Done()

			if err := r.sendAllDocuments(table); err != nil {
				r.pipe.Err <- err
				outerr = err
			} else if r.tail {
				start <- true
			}
			close(start)

		}(table)
	}
	wg.Wait()

	return nil
}

func (r *Rethinkdb) startupChangesForTable(table string, start <-chan bool, wg *sync.WaitGroup) error {
	wg.Add(1)
	defer wg.Done()
	if r.debug {
		fmt.Printf("getting a changes cursor for %s\n", table)
	}
	ccursor, err := gorethink.Table(table).Changes().Run(r.client)
	if err != nil {
		r.pipe.Err <- err
		return err
	}
	// wait until time to start sending changes
	<-start
	if err := r.sendChanges(table, ccursor); err != nil {
		r.pipe.Err <- err
		return err
	}
	return nil
}

func (r *Rethinkdb) sendAllDocuments(table string) error {
	if r.debug {
		fmt.Printf("sending all documents for %s\n", table)
	}

	cursor, err := gorethink.Table(table).Run(r.client)
	if err != nil {
		return err
	}
	defer cursor.Close()

	var doc map[string]interface{}
	for cursor.Next(&doc) {
		if stop := r.pipe.Stopped; stop {
			return nil
		}

		msg := message.NewMsg(message.Insert, r.prepareDocument(doc), r.computeNamespace(table))
		r.pipe.Send(msg)
	}

	if err := cursor.Err(); err != nil {
		return err
	}

	return nil
}

func (r *Rethinkdb) sendChanges(table string, ccursor *gorethink.Cursor) error {
	defer ccursor.Close()
	if r.debug {
		fmt.Printf("sending changes for %s\n", table)
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
			msg = message.NewMsg(message.Update, r.prepareDocument(change.NewVal), r.computeNamespace(table))
		} else if change.NewVal != nil {
			msg = message.NewMsg(message.Insert, r.prepareDocument(change.NewVal), r.computeNamespace(table))
		} else if change.OldVal != nil {
			msg = message.NewMsg(message.Delete, r.prepareDocument(change.OldVal), r.computeNamespace(table))
		}

		if msg != nil {
			r.pipe.Send(msg)
			if r.debug {
				fmt.Printf("msg: %#v\n", msg)
			}
		}
	}

	if err := ccursor.Err(); err != nil {
		return err
	}

	return nil
}

func (r *Rethinkdb) computeNamespace(table string) string {
	return strings.Join([]string{r.database, table}, ".")
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
	return r.pipe.Listen(r.applyOp, r.tableMatch)
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

	_, msgTable, err := msg.SplitNamespace()
	if err != nil {
		r.pipe.Err <- NewError(ERROR, r.path, fmt.Sprintf("rethinkdb error (msg namespace improperly formatted, must be database.table, got %s)", msg.Namespace), msg.Data)
		return msg, nil
	}
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
		resp, err = gorethink.Table(msgTable).Get(id).Delete().RunWrite(r.client)
	case message.Insert:
		resp, err = gorethink.Table(msgTable).Insert(doc).RunWrite(r.client)
	case message.Update:
		resp, err = gorethink.Table(msgTable).Insert(doc, gorethink.InsertOpts{Conflict: "replace"}).RunWrite(r.client)
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
