package file

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

type Adaptor struct {
	URI string
	FH  *os.File
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
	return "file"
}

func (r Adaptor) From(op ops.Op, namespace string, d interface{}) message.Msg {
	m := &Message{
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

func (r Adaptor) print(m message.Msg) error {
	b, err := message.MarshalData(m)
	if err != nil {
		return err
	}
	if strings.HasPrefix(r.URI, "stdout://") {
		fmt.Println(string(b))
		return nil
	}
	_, err = fmt.Fprintln(r.FH, string(b))
	return err
}

func (r Adaptor) Insert(m message.Msg) error {
	return r.print(m)
}

func (r Adaptor) Delete(m message.Msg) error {
	return r.print(m)
}

func (r Adaptor) Update(m message.Msg) error {
	return r.print(m)
}

func (r Adaptor) Command(m message.Msg) error {
	return r.print(m)
}

func (r Adaptor) MustUseFile(name string) message.Adaptor {
	a, err := r.UseFile(name)
	if err != nil {
		panic(err)
	}
	return a
}

func (r Adaptor) UseFile(uri string) (message.Adaptor, error) {
	if r.FH != nil {
		err := r.FH.Close()
		if err != nil {
			return r, err
		}
	}
	r.URI = uri
	name := strings.Replace(r.URI, "file://", "", 1)
	fh, err := os.Create(name)
	if err != nil {
		return r, err
	}
	r.FH = fh
	return r, err
}
