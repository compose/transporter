package rabbitmq

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/compose/transporter/log"
	"github.com/streadway/amqp"
)

const (
	testExchange = "transporter-tests"
)

var (
	defaultTestClient = &Client{
		uri: DefaultURI,
	}
	defaultSession *Session

	queuesToTest = []TestData{
		readerTestData, readerBadDataTest,
		writerTestData, writerTestData2, writerTestData3,
	}
)

type TestData struct {
	Queue       string
	RoutingKey  string
	InsertCount int
}

func setup() {
	log.Infoln("setting up tests")

	s, err := defaultTestClient.Connect()
	if err != nil {
		log.Errorf("unable to initialize connection to rabbitmq, %s", err)
		os.Exit(1)
	}
	defaultSession = s.(*Session)
	if err := defaultSession.channel.ExchangeDeclare(
		testExchange,
		"direct",
		true,
		false,
		false,
		false,
		nil); err != nil {
		log.Errorf("unable to declare exhange, %s", err)
		os.Exit(1)
	}
	for _, testData := range queuesToTest {
		setupData(testData, defaultSession.channel)
	}
}

func setupData(data TestData, ch *amqp.Channel) {
	if _, err := ch.QueueDeclare(data.Queue, false, false, false, false, nil); err != nil {
		log.Errorf("failed to declare queue (%s), may affect tests!, %s", data.Queue, err)
	}
	if _, err := ch.QueuePurge(data.Queue, true); err != nil {
		log.Errorf("failed to purge queue (%s), may affect tests!, %s", data.Queue, err)
	}
	if err := ch.QueueBind(data.Queue, data.RoutingKey, testExchange, false, nil); err != nil {
		log.Errorf("failed to bind queue (%s), may affect tests!, %s", data.Queue, err)
	}
	for i := 0; i < data.InsertCount; i++ {
		msg := amqp.Publishing{
			DeliveryMode: amqp.Transient,
			Timestamp:    time.Now(),
			ContentType:  "application/json",
			Body:         []byte(fmt.Sprintf(`{"id": %d, "message": "hello"}`, i)),
		}
		if err := ch.Publish(testExchange, data.RoutingKey, false, false, msg); err != nil {
			log.Errorf("failed to publish to queue (%s), may affect tests!, %s", data.Queue, err)
		}
	}
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func shutdown() {
	log.Infoln("shutting down tests")
	defaultTestClient.Close()
	log.Infoln("tests shutdown complete")
}
