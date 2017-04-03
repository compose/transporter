package rabbitmq

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/commitlog"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
	"github.com/streadway/amqp"
)

const (
	// DefaultAPIPort is the default API port for RabbitMQ
	DefaultAPIPort = 15672
)

var (
	_ client.Reader = &Reader{}
)

// Reader implements client.Reader by consuming messages from the cluster based on its configuration.
type Reader struct {
	uri     string
	apiPort int
}

func (r *Reader) Read(filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan client.MessageSet, error) {
		out := make(chan client.MessageSet)
		queues, err := r.listQueues(filterFn)
		if err != nil {
			return nil, err
		}
		go func(qs []string, session *Session) {
			defer close(out)
			var wg sync.WaitGroup
			for _, q := range queues {
				consumeChannel, cerr := session.conn.Channel()
				if cerr != nil {
					return
				}
				wg.Add(1)
				log.With("vhost", session.conn.Config.Vhost).With("queue", q).Infoln("consuming...")
				go consumeQueue(consumeChannel, q, &wg, done, out)
			}
			wg.Wait()
		}(queues, s.(*Session))
		return out, err
	}
}

func (r *Reader) listQueues(filterFn client.NsFilterFunc) ([]string, error) {
	u, _ := url.Parse(r.uri)
	httpScheme := "http"
	if u.Scheme == "amqps" {
		httpScheme = "https"
	}
	vhost := u.Path
	if vhost != "/" {
		vhost = vhost[1:]
	}
	apiURL := fmt.Sprintf("%s://%s:%d/api/queues/%s", httpScheme, u.Hostname(), r.apiPort, url.QueryEscape(vhost))
	log.With("apiURL", apiURL).Infoln("requesting queues")
	req, err := http.NewRequest(http.MethodGet, apiURL, nil)
	if u.User != nil {
		if pwd, ok := u.User.Password(); ok {
			req.SetBasicAuth(u.User.Username(), pwd)
		}
	}
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	defer resp.Body.Close()
	var queues []struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&queues); err != nil {
		return nil, err
	}
	out := make([]string, 0)
	for _, q := range queues {
		if filterFn(q.Name) {
			out = append(out, q.Name)
		}
	}
	return out, nil
}

// TODO: create a Consumer struct with fields to run this since we don't use anything from Reader
func consumeQueue(c *amqp.Channel, queue string, wg *sync.WaitGroup, done chan struct{}, out chan client.MessageSet) error {
	defer func() {
		log.With("queue", queue).Infoln("consuming complete")
		wg.Done()
	}()
	deliveries, err := c.Consume(queue, "transporter", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-done:
			return nil
		case msg := <-deliveries:
			var result map[string]interface{}
			if jerr := json.NewDecoder(bytes.NewReader(msg.Body)).Decode(&result); jerr != nil {
				log.Errorf("unable to decode message to JSON, %s", jerr)
				continue
			}
			out <- client.MessageSet{
				Msg:  message.From(ops.Insert, queue, result),
				Mode: commitlog.Sync,
			}
			msg.Ack(false)
		}
	}
}
