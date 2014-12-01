package adaptor

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
	gorethink "github.com/dancannon/gorethink"
)

type Rethinkdb struct {
	// pull these in from the config
	uri *url.URL

	// save time by setting these once
	database string
	table    string

	debug bool

	//
	pipe *pipe.Pipe

	// rethinkdb connection and options
	client *gorethink.Session
}

func NewRethinkdb(p *pipe.Pipe, extra Config) (StopStartListener, error) {
	var (
		conf dbConfig
		err  error
	)
	if err = extra.Construct(&conf); err != nil {
		return nil, err
	}

	u, err := url.Parse(conf.Uri)
	if err != nil {
		return nil, err
	}

	r := &Rethinkdb{
		uri:  u,
		pipe: p,
	}

	r.database, r.table, err = extra.splitNamespace()
	if err != nil {
		return r, err
	}
	r.debug = conf.Debug

	return r, nil
}

func (e *Rethinkdb) Start() error {
	return fmt.Errorf("Rethinkdb can't function as a source")
}

func (r *Rethinkdb) Listen() (err error) {
	r.client, err = r.setupClient()
	if err != nil {
		r.pipe.Err <- err
		return err
	}

	return r.pipe.Listen(r.applyOp)
}

func (r *Rethinkdb) Stop() error {
	r.pipe.Stop()
	return nil
}

func (r *Rethinkdb) applyOp(msg *message.Msg) (*message.Msg, error) {
	var (
		resp gorethink.WriteResponse
		err  error
	)

	switch msg.Op {
	case message.Delete:
		resp, err = gorethink.Table(r.table).Get(msg.IdAsString()).Delete().RunWrite(r.client)
	case message.Insert:
		resp, err = gorethink.Table(r.table).Insert(msg.Document()).RunWrite(r.client)
	case message.Update:
		resp, err = gorethink.Table(r.table).Insert(msg.DocumentWithId("id"), gorethink.InsertOpts{Conflict: "replace"}).RunWrite(r.client)
	}
	if err != nil {
		return msg, err
	}

	return msg, r.handleResponse(&resp)
}

func (r *Rethinkdb) setupClient() (*gorethink.Session, error) {
	// set up the clientConfig, we need host:port, username, password, and database name
	client, err := gorethink.Connect(gorethink.ConnectOpts{
		Address:     r.uri.Host,
		MaxIdle:     10,
		IdleTimeout: time.Second * 10,
	})
	if err != nil {
		return nil, fmt.Errorf("Unable to connect: %s", err)
	}

	gorethink.Db(r.database).TableDrop(r.table).RunWrite(client)
	gorethink.Db(r.database).TableCreate(r.table).RunWrite(client)

	client.Use(r.database)
	return client, nil
}

// handleresponse takes the rethink response and turn it into something we can consume elsewhere
func (r *Rethinkdb) handleResponse(resp *gorethink.WriteResponse) error {
	if resp.Errors != 0 {
		if !strings.Contains(resp.FirstError, "Duplicate primary key") { // we don't care about this error
			if r.debug {
				fmt.Printf("Reported %d errors\n", resp.Errors)
			}
			return fmt.Errorf("%s\n%s", "Problem inserting docs", resp.FirstError)
		}
	}
	return nil
}
