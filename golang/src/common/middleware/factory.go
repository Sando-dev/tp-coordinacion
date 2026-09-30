package middleware

import (
	"strconv"
	// m "github.com/7574-sistemas-distribuidos/tp-coordinacion/golang/src/common/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func createConnectionAndChannel(connectionSettings ConnSettings) (*amqp.Connection, *amqp.Channel, error) {

	url := "amqp://guest:guest@" +
		connectionSettings.Hostname + ":" +
		strconv.Itoa(connectionSettings.Port) + "/"

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, ErrMessageMiddlewareDisconnected
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, ErrMessageMiddlewareDisconnected
	}

	return conn, ch, nil
}

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	conn, ch, err := createConnectionAndChannel(connectionSettings)
	if err != nil {
		return nil, err
	}

	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durability
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, ErrMessageMiddlewareDisconnected
	}

	queueMiddleware := NewQueueMiddleware(conn, ch, queueName)

	return queueMiddleware, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	conn, ch, err := createConnectionAndChannel(connectionSettings)
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchange, // name
		"direct", // type
		false,    // durability
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, ErrMessageMiddlewareDisconnected
	}

	exchangeMiddleware := NewExchangeMiddleware(conn, ch, exchange, keys)

	return exchangeMiddleware, nil
}
