package queueing

import (
	"github.com/streadway/amqp"
	"github.com/danmaina/logger"
)

type RabbitMQ struct {
	Host             string `yaml:"host" json:"host"`
	Port             string `yaml:"port" json:"port"`
	VirtualHost      string `yaml:"virtualHost" json:"virtualHost"`
	Username         string `yaml:"username" json:"username"`
	Password         string `yaml:"password" json:"password"`
	ConsumerCount    int    `yaml:"consumerCount" json:"consumerCount"`
	PrefetchCount    int    `yaml:"prefetchCount" json:"prefetchCount"`
	PrefetchSize     int    `yaml:"prefetchSize" json:"prefetchSize"`
	QueueNamePrefix  string `yaml:"queueNamePrefix" json:"queueNamePrefix"`
	QueueName        string `yaml:"queueName" json:"queueName"`
	IsQosGlobal      bool   `yaml:"isQosGlobal" json:"isQosGlobal"`
	IsQueueDurable   bool   `yaml:"isQueueDurable" json:"isQueueDurable"`
	IsQueueExclusive bool   `yaml:"isQueueExclusive" json:"isQueueExclusive"`
}

func (r RabbitMQ) createRabbitConnection() (*amqp.Connection, error) {

	conString := "amqp://" + r.Username + ":" + r.Password + "@" + r.Host + ":" + r.Port + "/" + r.VirtualHost + ""

	conn, err := amqp.Dial(conString)

	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (r RabbitMQ) createChannel(c *amqp.Connection) (*amqp.Channel, error) {
	return c.Channel()
}

func InitializeAsyncRabbitConsumers(mq RabbitMQ, processingFunc func(msg amqp.Delivery)(string, error)) *amqp.Connection {

	rabbitConnection, errGettingRabbitConn := mq.createRabbitConnection()

	if errGettingRabbitConn != nil {
		logger.FATAL("Could Not get a connection from Rabbit: ", errGettingRabbitConn)
	}

	rabbitErr := <-consumeFromRabbit(rabbitConnection, mq, processingFunc)

	for rabbitErr != nil {
		logger.ERR("RabbitMQ Connection Closed! Reconnecting: ", rabbitErr)

		rabbitConnection, errGettingRabbitConn = mq.createRabbitConnection()
		rabbitErr = <-consumeFromRabbit(rabbitConnection, mq, processingFunc)
	}

	return rabbitConnection
}

func consumeFromRabbit(rabbitConnection *amqp.Connection, r RabbitMQ, processingFunc func(msg amqp.Delivery)(string, error)) chan *amqp.Error {

	// Get Channels and Create consumers
	for i := 0; i < r.ConsumerCount; i++ {
		go func(i int) {
			rabbitChannel, errCreatingChannel := r.createChannel(rabbitConnection)
			if errCreatingChannel != nil {
				logger.FATAL("Could Not Create RabbitMQ Channel: ", errCreatingChannel)
			}

			errConsumingFromQueue := consumeFromQueue(
				rabbitChannel,
				r.PrefetchCount,
				r.PrefetchSize,
				r.QueueNamePrefix+"."+r.QueueName,
				r.IsQosGlobal,
				r.IsQueueDurable,
				r.IsQueueExclusive,
				processingFunc)

			if errConsumingFromQueue != nil {
				logger.FATAL("Could Not Consume From Queue: ", errConsumingFromQueue)
			}

		}(i)
	}

	errChan := make(chan *amqp.Error)
	rabbitConnection.NotifyClose(errChan)

	return errChan
}


// Create A consumer that Consumes RabbitMQ Balance Enquiry Messages From Queue
func consumeFromQueue(ch *amqp.Channel, prefetchCount, prefetchSize int, queueName string, qosGlobal, queueDurable, queueExclusive bool, processMessage func(message amqp.Delivery)(string, error)) error {

	// Initialize Queue
	q, errQDeclare := ch.QueueDeclare(
		queueName,
		queueDurable,
		false,
		queueExclusive,
		false,
		nil,
	)

	logger.DEBUG("Declared RabbitMQ Queue: ", q)

	if errQDeclare != nil {
		logger.ERR("Could Not Create RabbitMQ Queue With Error: ", errQDeclare)
		return errQDeclare
	}

	// Consumer Messages Forever
	msgs, errConsuming := ch.Consume(
		q.Name,
		"",
		false,
		queueExclusive,
		false,
		false,
		nil,
	)

	if errConsuming != nil {
		logger.ERR("Could Not Consume From Queue with error: ", errConsuming)
		return errConsuming
	}

	msgConsumerChannel := make(chan bool)

	go func() {
		for msg := range msgs {

			logger.INFO("Queue Processing Request: ", string(msg.Body))

			// TODO: Add function to be called

			resultString, resultErr := processMessage(msg)


			if resultErr != nil {
				logger.ERR("Error calling Results API: ", resultErr)
			}

			logger.INFO("Response From Queue Processing: ", resultString)

			// Ack to RabbitMQ
			logger.ERR(msg.Ack(true))
		}
	}()

	<-msgConsumerChannel

	return nil
}