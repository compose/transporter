package etcd

import (
	"fmt"
	"time"

	"golang.org/x/net/context"

	"git.compose.io/compose/transporter/pkg/message"
	"git.compose.io/compose/transporter/pkg/message/data"
	"git.compose.io/compose/transporter/pkg/message/ops"
	"github.com/coreos/etcd/client"
)

type Adaptor struct {
	client client.Client
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
	return "etcd"
}

func (r Adaptor) From(op ops.Op, namespace string, d data.Data) message.Msg {
	return &Message{
		Operation: op,
		TS:        time.Now().Unix(),
		NS:        namespace,
		MapData:   d,
	}
}

func genKey(m message.Msg, dataKey string) (string, error) {
	key, ns, err := message.SplitNamespace(m)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("/%s/%s/%s", key, ns, dataKey), nil
}

func (r Adaptor) Insert(m message.Msg) error {
	kapi := client.NewKeysAPI(r.client)
	for k, v := range m.Data() {
		key, err := genKey(m, k)
		if err != nil {
			return err
		}
		_, err = kapi.Create(context.Background(), key, fmt.Sprintf("%v", v))
		if err != nil {
			return err
		}
	}
	return nil
}

func (r Adaptor) Delete(m message.Msg) error {
	kapi := client.NewKeysAPI(r.client)
	for k := range m.Data() {
		key, err := genKey(m, k)
		if err != nil {
			return err
		}
		_, err = kapi.Delete(context.Background(), key, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r Adaptor) Update(m message.Msg) error {
	kapi := client.NewKeysAPI(r.client)
	for k, v := range m.Data() {
		key, err := genKey(m, k)
		if err != nil {
			return err
		}
		_, err = kapi.Set(context.Background(), key, fmt.Sprintf("%v", v), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r Adaptor) UseClient(c client.Client) message.Adaptor {
	r.client = c
	return r
}
