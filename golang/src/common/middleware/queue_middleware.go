package middleware

import (
	"context"
	// m "github.com/7574-sistemas-distribuidos/tp-coordinacion/golang/src/common/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	connection  *amqp.Connection
	channel     *amqp.Channel
	queueName   string
	consumerTag string
	isConsuming	bool
}

func NewQueueMiddleware(connection *amqp.Connection, channel *amqp.Channel, queueName string) *QueueMiddleware {
	return &QueueMiddleware{
		connection:		connection,
		channel:     	channel,
		queueName:   	queueName,
		consumerTag: 	queueName + "-consumer",
		isConsuming:	false,	
	}
}

func (q *QueueMiddleware) isDisconnected() bool {
	return q.connection.IsClosed() || q.channel.IsClosed()
}


func (q *QueueMiddleware) Close() error {
	channelErr := q.channel.Close()
	connectionErr := q.connection.Close()
	if channelErr != nil || connectionErr != nil {
		return ErrMessageMiddlewareClose
	}

	return nil
}

func (q *QueueMiddleware) Send(msg Message) error {
	if q.isDisconnected() {
		return ErrMessageMiddlewareDisconnected
	}

	ctx := context.Background()
	err := q.channel.PublishWithContext(ctx,
		"",          // exchange
		q.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		if q.isDisconnected() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}
	return nil
}

func (q *QueueMiddleware) StartConsuming(
	callbackFunc func(msg Message, ack func(), nack func()),
) error {

	if q.isDisconnected() {
		return ErrMessageMiddlewareDisconnected
	}


	msgs, err := q.channel.Consume(
		q.queueName,   // queue
		q.consumerTag, // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		if q.isDisconnected() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	q.isConsuming = true

	// go func() {
	// 	for d := range msgs {
	// 		body := string(d.Body)
	// 		msg := Message{
	// 			Body: body,
	// 		}
	// 		ack := func() {
	// 			d.Ack(false)
	// 		}
	// 		nack := func() {
	// 			d.Nack(false, false)
	// 		}

	// 		callbackFunc(msg, ack, nack)
	// 	}
	// }()

	for d := range msgs {
		body := string(d.Body)
		msg := Message{
			Body: body,
		}
		ack := func() {
			d.Ack(false)
		}
		nack := func() {
			d.Nack(false, false)
		}

		callbackFunc(msg, ack, nack)
	}

	return nil
}

func (q *QueueMiddleware) StopConsuming() error {
	if !q.isConsuming {
		return nil
	}

	if q.isDisconnected() {
		return ErrMessageMiddlewareDisconnected
	}

	err := q.channel.Cancel(q.consumerTag, false)
	if err != nil {
		if q.isDisconnected() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareDisconnected
	}

	q.isConsuming = false
	return nil
}