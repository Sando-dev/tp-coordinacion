package middleware

import (
	"context"
	// m "github.com/7574-sistemas-distribuidos/tp-coordinacion/golang/src/common/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	connection  	*amqp.Connection
	channel     	*amqp.Channel
	exchangeName  	string
	keys 			[]string
	consumerTag 	string
	isConsuming  	bool
}

func NewExchangeMiddleware(connection *amqp.Connection, channel *amqp.Channel, exchangeName string, keys []string) *ExchangeMiddleware {
	return &ExchangeMiddleware{
		connection:  	connection,
		channel:     	channel,
		exchangeName:   exchangeName,
		keys:			keys,
		consumerTag: 	exchangeName + "-consumer",
		isConsuming:	false,
	}
}

func (e *ExchangeMiddleware) isDisconnected() bool {
	return e.connection.IsClosed() || e.channel.IsClosed()
}

func (e *ExchangeMiddleware) Close() error {
	channelErr := e.channel.Close()
	connectionErr := e.connection.Close()

	if channelErr != nil || connectionErr != nil {
		return ErrMessageMiddlewareClose
	}

	return nil
}

func (e *ExchangeMiddleware) Send(msg Message) error {
	if e.isDisconnected() {
		return ErrMessageMiddlewareDisconnected
	}

	ctx := context.Background()

	for _, key := range e.keys {
		err := e.channel.PublishWithContext(
			ctx,
			e.exchangeName,
			key,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			},
		)

		if err != nil {
			if e.isDisconnected() {
				return ErrMessageMiddlewareDisconnected
			}
			return ErrMessageMiddlewareMessage
		}
	}

	return nil
}

func (e *ExchangeMiddleware) StartConsuming(
	callbackFunc func(msg Message, ack func(), nack func()),
) error {

	if e.isDisconnected() {
		return ErrMessageMiddlewareDisconnected
	}

	q, err := e.channel.QueueDeclare(
		"",    // name
		false, // durability
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
    )
	if err != nil {
		if e.isDisconnected() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	for _, key := range e.keys {
		err = e.channel.QueueBind(
			q.Name,	// queue name
			key,			// routing key
			e.exchangeName,	// exchange
			false,
			nil)
		if err != nil {
			if e.isDisconnected() {
				return ErrMessageMiddlewareDisconnected
			}
			return ErrMessageMiddlewareMessage
		}
	}

	msgs, err := e.channel.Consume(
		q.Name,	 		// queue
		e.consumerTag,  // consumer
		false,   		// auto ack
		false,  		// exclusive
		false,  		// no local
		false,  		// no wait
		nil,    		// args
	)
	if err != nil {
		if e.isDisconnected() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}
	
	e.isConsuming = true

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

func (e *ExchangeMiddleware) StopConsuming() error {
	if !e.isConsuming {
		return nil
	}

	if e.isDisconnected() {
		return ErrMessageMiddlewareDisconnected
	}

	err := e.channel.Cancel(e.consumerTag, false)
	if err != nil {
		if e.isDisconnected() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareDisconnected
	}

	e.isConsuming = false
	return nil
}