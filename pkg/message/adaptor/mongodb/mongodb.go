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
var _ message.Commandable = Adaptor{}
var _ message.Deletable = Adaptor{}
var _ message.Updatable = Adaptor{}

func init() {
	a := Adaptor{}
	message.Register(a.Name(), a)
}

func (r Adaptor) Name() string {
	return "mongodb"
}

func (r Adaptor) From(op ops.Op, namespace string, d interface{}) message.Msg {
	m := &MongoMessage{
		Operation: op,
		TS:        time.Now().Unix(),
		NS:        namespace,
	}
	switch d.(type) {
	case map[string]interface{}, bson.M:
		m.BSONData = d.(data.BSONData)
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

func (r Adaptor) MustUseSession(sess interface{}) message.Adaptor {
	a, err := r.UseSession(sess)
	if err != nil {
		panic(err)
	}
	return a
}

func (r Adaptor) UseSession(sess interface{}) (message.Adaptor, error) {
	if c, ok := sess.(*mgo.Session); ok {
		r.sess = c
		return r, nil
	}
	return r, fmt.Errorf("session is not valid connection type: %T", sess)
}
