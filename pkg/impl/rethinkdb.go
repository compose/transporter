package impl

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
	// pull these in from the node
	uri *url.URL

	// save time by setting these once
	database string
	table    string

	//
	pipe pipe.Pipe

	// rethinkdb connection and options
	client *gorethink.Session
}

func NewRethinkdb(p pipe.Pipe, extra ExtraConfig) (*Rethinkdb, error) {
	var (
		conf RethinkdbConfig
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

	r.database, r.table, err = r.splitNamespace(conf.Namespace)
	if err != nil {
		return r, err
	}

	return r, nil
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

	switch msg.Op {
	case message.Delete:
		_, _ = gorethink.Table(r.table).Get(msg.IdAsString()).Delete().RunWrite(r.client)
	case message.Insert:
		_, _ = gorethink.Table(r.table).Insert(msg.Document()).RunWrite(r.client)
	case message.Update:
		_, _ = gorethink.Table(r.table).Insert(msg.DocumentWithId("id"), gorethink.InsertOpts{Conflict: "replace"}).RunWrite(r.client)
	}

	return msg, nil
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

/*
 * split a rethinkdb namespace into a database and table
 */
func (r *Rethinkdb) splitNamespace(namespace string) (string, string, error) {
	fields := strings.SplitN(namespace, ".", 2)

	if len(fields) != 2 {
		return "", "", fmt.Errorf("malformed rethinkdb namespace.")
	}
	return fields[0], fields[1], nil
}

// RethinkdbConfig options
type RethinkdbConfig struct {
	Uri       string `json:"uri"`       // the database uri
	Namespace string `json:"namespace"` // namespace
	Debug     bool   `json:"debug"`     // debug mode
}
