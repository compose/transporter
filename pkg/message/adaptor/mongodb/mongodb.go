package mongodb

import (
	"fmt"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

type Adaptor struct {
	sess *mgo.Session
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
	return "mongo"
}

func (r Adaptor) From(op ops.Op, namespace string, d interface{}) message.Msg {
	m := &MongoMessage{
		Operation: op,
		TS:        time.Now().Unix(),
		NS:        namespace,
	}
	switch d.(type) {
	case map[string]interface{}:
		m.BSONData = data.BSONData(d.(map[string]interface{}))
	case bson.M:
		m.BSONData = data.BSONData(d.(bson.M))
	}
	return m
}

func (r Adaptor) Insert(m message.Msg) error {
	db, coll, err := message.SplitNamespace(m)
	if err != nil {
		return err
	}
	return r.sess.DB(db).C(coll).Insert(m.Data())
}

func (r Adaptor) BulkInsert(db string, coll string, m ...message.Msg) error {
	if len(m) == 0 {
		return nil
	}
	ins := make([]interface{}, len(m), len(m))
	for i := range m {
		ins[i] = m[i].Data()
	}
	return r.sess.DB(db).C(coll).Insert(ins...)
}

func (r Adaptor) Delete(m message.Msg) error {
	db, coll, err := message.SplitNamespace(m)
	if err != nil {
		return err
	}
	return r.sess.DB(db).C(coll).Remove(m.Data())
}

func (r Adaptor) Update(m message.Msg) error {
	db, coll, err := message.SplitNamespace(m)
	if err != nil {
		return err
	}
	return r.sess.DB(db).C(coll).Update(bson.M{"_id": m.Data().(data.BSONData).AsMap()["_id"]}, m.Data())
}

func (r Adaptor) MustUseSession(sess interface{}) Adaptor {
	a, err := r.UseSession(sess)
	if err != nil {
		panic(err)
	}
	return a
}

func (r Adaptor) UseSession(sess interface{}) (Adaptor, error) {
	if c, ok := sess.(*mgo.Session); ok {
		r.sess = c
		return r, nil
	}
	return r, fmt.Errorf("session is not valid connection type: %T", sess)
}
