package rethinkdb

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
	"github.com/dancannon/gorethink"
)

type Adaptor struct {
	conn *gorethink.Session
}

var _ message.Adaptor = Adaptor{}
var _ message.Insertable = Adaptor{}
var _ message.Deletable = Adaptor{}
var _ message.Updatable = Adaptor{}

func init() {
	a := Adaptor{}
	message.Register(a.Name(), a)
}

func (r Adaptor) Name() string {
	return "rethinkdb"
}

func (r Adaptor) From(op ops.Op, namespace string, d interface{}) message.Msg {
	m := &RethinkMessage{
		Operation: op,
		TS:        time.Now().Unix(),
		NS:        namespace,
	}
	switch d.(type) {
	case map[string]interface{}:
		m.MapData = data.MapData(d.(map[string]interface{}))
	case bson.M:
		m.MapData = data.MapData(d.(bson.M))
	case data.MapData:
		m.MapData = d.(data.MapData)
	}
	return m
}

func (r Adaptor) Insert(m message.Msg) error {
	_, msgTable, err := message.SplitNamespace(m)
	if err != nil {
		return err
	}
	resp, err := gorethink.Table(msgTable).Insert(m.Data()).RunWrite(r.conn)
	err = handleResponse(&resp)
	return err
}

func (r Adaptor) Delete(m message.Msg) error {
	_, msgTable, err := message.SplitNamespace(m)
	if err != nil {
		return err
	}
	resp, err := gorethink.Table(msgTable).Get(m.ID()).Delete().RunWrite(r.conn)
	err = handleResponse(&resp)
	return err
}

func (r Adaptor) Update(m message.Msg) error {
	_, msgTable, err := message.SplitNamespace(m)
	if err != nil {
		return err
	}
	resp, err := gorethink.Table(msgTable).Insert(m.Data(), gorethink.InsertOpts{Conflict: "replace"}).RunWrite(r.conn)
	err = handleResponse(&resp)
	return err
}

// handleresponse takes the rethink response and turn it into something we can consume elsewhere
func handleResponse(resp *gorethink.WriteResponse) error {
	if resp.Errors != 0 {
		if !strings.Contains(resp.FirstError, "Duplicate primary key") { // we don't care about this error
			return fmt.Errorf("%s\n%s", "problem inserting docs", resp.FirstError)
		}
	}
	return nil
}

func (r Adaptor) MustUseSession(conn interface{}) message.Adaptor {
	a, err := r.UseSession(conn)
	if err != nil {
		panic(err)
	}
	return a
}

func (r Adaptor) UseSession(conn interface{}) (message.Adaptor, error) {
	if c, ok := conn.(*gorethink.Session); ok {
		r.conn = c
		return r, nil
	}
	return r, fmt.Errorf("conn is not valid connection type: %T", conn)
}
