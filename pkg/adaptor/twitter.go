package adaptor

import (
	"encoding/json"
	"fmt"

	"github.com/codepope/transporter/pkg/message"
	"github.com/codepope/transporter/pkg/pipe"
	"github.com/darkhelmet/twitterstream"
	"github.com/fatih/structs"
)

// Twitter is an adaptor that can only be used as a
// source because Twitter don't like streaming bots
type Twitter struct {
	consumerKey    string
	consumerSecret string
	accessToken    string
	accessSecret   string
	keywords       string

	// These are for sending our messages onwards
	pipe *pipe.Pipe
	path string

	// Twitterstream keeps a connection and options
	client *twitterstream.Client
}

// NewTwitter returns a Twitter Adaptor
func NewTwitter(p *pipe.Pipe, path string, extra Config) (StopStartListener, error) {
	var (
		conf TwitterConfig
		err  error
	)
	if err = extra.Construct(&conf); err != nil {
		return nil, NewError(CRITICAL, path, fmt.Sprintf("Can't configure adaptor (%s)", err.Error()), nil)
	}

	return &Twitter{
		consumerKey:    conf.ConsumerKey,
		consumerSecret: conf.ConsumerSecret,
		accessToken:    conf.AccessToken,
		accessSecret:   conf.AccessSecret,
		keywords:       conf.Keywords,
		pipe:           p,
		path:           path,
	}, nil
}

// Start the twitter adaptor
func (d *Twitter) Start() (err error) {
	defer func() {
		d.Stop()
	}()

	return d.readTwitter()
}

// Listen starts the listen loop
func (d *Twitter) Listen() (err error) {
	return nil
}

// Stop the adaptor
func (d *Twitter) Stop() error {
	// d.pipe.Stop()
	return nil
}

// read each message from twitter
func (d *Twitter) readTwitter() (err error) {

	d.client = twitterstream.NewClient(d.consumerKey, d.consumerSecret, d.accessToken, d.accessSecret)

	var conn *twitterstream.Connection

	if d.keywords == "" {
		fmt.Println("Sampling")
		conn, err = d.client.Sample()
	} else {
		fmt.Printf("Tracking %s", d.keywords)
		conn, err = d.client.Track(d.keywords)
	}

	for {
		if tweet, err := conn.Next(); err == nil {
			doc := structs.Map(tweet)
			msg := message.NewMsg(message.Insert, doc)
			if msg != nil {
				fmt.Printf("msg: %#v\n", msg)
				d.pipe.Send(msg)
			}
		} else {
			break
		}

	}
	return nil
}

/*
 * dump each message to the file
 */
func (d *Twitter) dumpMessage(msg *message.Msg) (*message.Msg, error) {
	var line string

	if msg.IsMap() {
		ba, err := json.Marshal(msg.Map())
		if err != nil {
			d.pipe.Err <- NewError(ERROR, d.path, fmt.Sprintf("Can't unmarshal document (%s)", err.Error()), msg.Data)
			return msg, nil
		}
		line = string(ba)
	} else {
		line = fmt.Sprintf("%v", msg.Data)
	}

	fmt.Println(line)

	return msg, nil
}

// TwitterConfig is used to configure the File Adaptor,
type TwitterConfig struct {
	ConsumerKey    string `json:"consumerKey" doc:"The Twitter ConsumerKey"`
	ConsumerSecret string `json:"consumerSecret" doc:"The Twitter ConsumerSecret"`
	AccessToken    string `json:"accessToken" doc:"The Twitter AccessToken"`
	AccessSecret   string `json:"accessSecret" doc:"The Twitter AccessSecret"`
	Keywords       string `json:"keywords" doc:"Keywords,Comma-separated,Leave Empty for Twitter Sample"`
}
