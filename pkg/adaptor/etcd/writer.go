package etcd

import (
	"context"
	"fmt"

	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/ops"

	eclient "github.com/coreos/etcd/client"
)

var (
	_ client.Writer = &Writer{}
)

// Writer implements client.Writer for use with etcd
type Writer struct {
	rootKey  string
	writeMap map[ops.Op]func(message.Msg, eclient.KeysAPI) error
}

func newWriter(rootKey string) *Writer {
	w := &Writer{
		rootKey: rootKey,
	}
	w.writeMap = map[ops.Op]func(message.Msg, eclient.KeysAPI) error{
		ops.Insert: w.insertMsg,
		ops.Update: w.updateMsg,
		ops.Delete: w.deleteMsg,
	}
	return w
}

func (w *Writer) Write(msg message.Msg) func(client.Session) error {
	return func(s client.Session) error {
		w, ok := w.writeMap[msg.OP()]
		if !ok {
			log.Infof("no function registered for operation, %s\n", msg.OP())
			return nil
		}
		return w(msg, keysAPI(s.(*Session)))
	}
}

func keysAPI(s *Session) eclient.KeysAPI {
	return eclient.NewKeysAPI(s.Client)
}

func (w *Writer) genKey(m message.Msg, dataKey string) (string, error) {
	_, ns, err := message.SplitNamespace(m)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("/%s/%s/%s", w.rootKey, ns, dataKey), nil
}

func (w *Writer) insertMsg(msg message.Msg, api eclient.KeysAPI) error {
	for k, v := range msg.Data() {
		key, err := w.genKey(msg, k)
		if err != nil {
			return err
		}
		_, err = api.Create(context.Background(), key, fmt.Sprintf("%v", v))
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) updateMsg(msg message.Msg, api eclient.KeysAPI) error {
	for k, v := range msg.Data() {
		key, err := w.genKey(msg, k)
		if err != nil {
			return err
		}
		_, err = api.Set(context.Background(), key, fmt.Sprintf("%v", v), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) deleteMsg(msg message.Msg, api eclient.KeysAPI) error {
	for k := range msg.Data() {
		key, err := w.genKey(msg, k)
		if err != nil {
			return err
		}
		_, err = api.Delete(context.Background(), key, nil)
		if err != nil {
			return err
		}
	}
	return nil
}
