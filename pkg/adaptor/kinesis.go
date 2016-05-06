package adaptor

import (
	"fmt"
	"sync"
	"time"

	kn "github.com/sendgridlabs/go-kinesis"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
)

const (
	TIMEOUT_BUFFER_SIZE int = 30
)

type Kinesis struct {
	pipe              *pipe.Pipe
	streamName        string
	ksis              kn.KinesisClient
	shardIteratorType string
	path              string
	regionName        string
	awsAccessKey      string
	awsSecretKey      string
}

var sampleConfig = `
- stream:
    type: kinesis
    awsAccessKEY : XXX
    awsSecretKey : XXX
    streamName : test
    shardIteratorType: TRIM_HORIZON
`

func (k *Kinesis) Listen() (err error) {
	return nil
}

// Stop the adaptor
func (k *Kinesis) Stop() error {
	k.pipe.Stop()
	return nil
}

func NewKinesis(p *pipe.Pipe, path string, extra Config) (StopStartListener, error) {

	var (
		conf KinesisConfig
		err  error
	)

	if err = extra.Construct(&conf); err != nil {
		return nil, err
	}

	if conf.AwsAccessKey == "" || conf.AwsSecretKey == "" {
		return nil, fmt.Errorf("both awsAccessKEY and awsSecretKey required, but missing ")
	}
	// os.Setenv("AWS_ACCESS_KEY_ID", conf.Awsaccesskey)
	// os.Setenv("AWS_SECRET_ACCESS_KEY", conf.Awssecretkey)
	//auth, err := kn.NewAuthFromEnv()
	auth := kn.NewAuth(conf.AwsAccessKey, conf.AwsSecretKey)

	if err != nil {
		return nil, NewError(CRITICAL, path, fmt.Sprintf("Unable to retrieve authentication credentials from the environment: %v", err.Error()), nil)
	}
	ksis := kn.New(auth, conf.RegionName) 

	k := &Kinesis{
		pipe:              p,
		streamName:        conf.StreamName,
		ksis:              ksis,
		shardIteratorType: conf.ShardIteratorType,
		path:              path,
		regionName:        conf.RegionName,
	}

	return k, nil

}

// Start the kinesis adaptor
func (k *Kinesis) Start() (err error) {
	defer func() {
		k.Stop()
	}()

	fmt.Println("start")
	timeout := make(chan bool, TIMEOUT_BUFFER_SIZE)
	resp3 := &kn.DescribeStreamResp{}
	var wg sync.WaitGroup

	for {

		args := kn.NewArgs()
		args.Add("StreamName", k.streamName)
		resp3, err = k.ksis.DescribeStream(args)
		
		if err != nil {
			k.pipe.Err <- err
			return err
		}
		
		fmt.Printf("DescribeStream: %v\n", resp3)

		if resp3.StreamDescription.StreamStatus != "ACTIVE" {
			fmt.Println("[Error] : Stream is not active.")
			time.Sleep(4 * time.Second)
			timeout <- true
		} else {
			break
		}

	}

	for _, shard := range resp3.StreamDescription.Shards {
		wg.Add(1)
		go k.getRecords(shard.ShardId, &wg)
	}

	wg.Wait()

	return
}

//This function gets the data from kinesis streams according to the mentioned shardId and shardIteratorType.
//Capable to get the live data from streams.
//Capable to fetch the old data from streams. 
func (k *Kinesis) getRecords(shardId string, wg *sync.WaitGroup) (err error) {
	args := kn.NewArgs()
	args.Add("StreamName", k.streamName)
	args.Add("ShardId", shardId)
	args.Add("ShardIteratorType", k.shardIteratorType)
	resp10, _ := k.ksis.GetShardIterator(args)
	shardIterator := resp10.ShardIterator

	for {
		args = kn.NewArgs()
		args.Add("ShardIterator", shardIterator)
		resp11, err := k.ksis.GetRecords(args)

		if err != nil {
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		if len(resp11.Records) > 0 {
			fmt.Printf("GetRecords Data BEGIN\n")
			var doc []byte

			for _, d := range resp11.Records {
				doc = d.GetData()                                                                         
				k.pipe.Send(message.NewMsg(message.Insert, doc, fmt.Sprintf("kinesis.%s", k.streamName))) 
			}
		} else if resp11.NextShardIterator == "" || shardIterator == resp11.NextShardIterator || err != nil {
			k.pipe.Err <- NewError(ERROR, k.path, fmt.Sprintf("GetRecords data error (%s)", err.Error()), nil)
			return err
		}

		shardIterator = resp11.NextShardIterator
		time.Sleep(1000 * time.Millisecond)
	}
	wg.Done()
	return nil
}

// Config is used to configure the kinesis Adaptor
type KinesisConfig struct {
	AwsAccessKey      string `json:"awsaccesskey" doc:"aws authentication"`
	AwsSecretKey      string `json:"awssecretkey" doc:"aws authentication"`
	StreamName        string `json:"streamname" doc:"kinesis stream name to get the data"`
	ShardIteratorType string `json:"sharditeratortype" doc:"sharding type to get the data from stream"`
	RegionName        string `json:"awsregionname" doc:"region name to get the data"`
}
