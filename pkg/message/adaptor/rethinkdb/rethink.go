package rethinkdb

import (
	"fmt"
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
var _ message.Commandable = Adaptor{}
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
	}
	return m
}

func (r Adaptor) Insert(m message.Msg) error {
	return nil
}

func (r Adaptor) Delete(m message.Msg) error {
	return nil
}

func (r Adaptor) Update(m message.Msg) error {
	return nil
}

func (r Adaptor) Command(m message.Msg) error {
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
