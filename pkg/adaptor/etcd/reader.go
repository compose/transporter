package etcd

import (
	"context"
	"fmt"
	"strings"

	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"

	eclient "github.com/coreos/etcd/client"
)

var (
	_ client.Reader = &Reader{}
)

// Reader implements client.Reader for the use of iterating an etcd directory.
type Reader struct {
	rootKey string
}

type nodeMsg struct {
	node *eclient.Node
	path string
}

func newReader(rootKey string) client.Reader {
	return &Reader{rootKey}
}

func (r *Reader) Read(filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan message.Msg, error) {
		out := make(chan message.Msg)
		session := s.(*Session)
		log.With("rootKey", r.rootKey).Infoln("starting Read func")
		results := make(chan *nodeMsg)
		go func() {
			r.walkNode(r.rootAsPath(), session, results)
			close(results)
		}()
		go func() {
			defer close(out)
			directoryMap := make(map[string]map[string]interface{})
			for {
				select {
				case <-done:
					return
				case result, ok := <-results:
					if !ok {
						return
					}
					if result.node.Dir {
						nodepath := strings.TrimPrefix(result.path, r.rootAsPath())
						nodepath = strings.Replace(nodepath, "/", "_", -1)
						if nodepath == "" {
							nodepath = "root"
						}
						out <- message.From(ops.Insert, fmt.Sprintf("%s.%s", r.rootKey, nodepath), data.Data(directoryMap[result.path]))
						continue
					}
					if !filterFn(result.node.Key) {
						continue
					}
					idx := strings.LastIndexByte(result.node.Key, '/')
					var key = "/"
					if idx > 0 {
						key = result.node.Key[:idx]
					}
					dir, ok := directoryMap[key]
					if !ok {
						dir = make(map[string]interface{})
						directoryMap[key] = dir
					}
					nodekey := strings.TrimPrefix(result.node.Key, key)
					nodekey = strings.TrimPrefix(nodekey, "/")
					dir[nodekey] = result.node.Value
				}
			}
		}()
		return out, nil
	}
}

func (r *Reader) rootAsPath() string {
	return fmt.Sprintf("/%s", r.rootKey)
}

// walkNode starts at the provided path and either sends a value or calls walk if a directory
func (r *Reader) walkNode(path string, s *Session, results chan *nodeMsg) error {
	kc := eclient.NewKeysAPI(s.Client)
	resp, err := kc.Get(context.Background(), path, &eclient.GetOptions{})
	if err != nil {
		return err
	}
	for _, n := range resp.Node.Nodes {
		r.walkNode(n.Key, s, results)
	}
	results <- &nodeMsg{resp.Node, path}
	if resp.Node.Dir {
		log.With("key", path).Infoln("done walking dir")
	}
	return nil
}
