package notifier

import (
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitOpts func(*RabbitNotifer)

type RabbitNotifer struct {
	amqpUrl string
	conn    *amqp.Connection
	channel *amqp.Channel
}

func (rn *RabbitNotifer) Close() error {
	chanErr := rn.channel.Close()
	connErr := rn.conn.Close()

	return errors.Join(chanErr, connErr)
}

func (rn *RabbitNotifer) Notify(restaurantID, orderID string) error {
	// Declare the queue in case it doesn't exist
	// This action is idempotent as per the API
	q, err := rn.channel.QueueDeclare(
		"order_notifications_"+restaurantID,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// Send notification
	err = rn.channel.Publish(
		"",     // exchange
		q.Name, // queue
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(orderID),
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func WithURL(url string) RabbitOpts {
	return func(rn *RabbitNotifer) {
		rn.amqpUrl = url
	}
}

func NewRabbitNotifier(opts ...RabbitOpts) (*RabbitNotifer, error) {
	rn := &RabbitNotifer{}

	for _, opt := range opts {
		opt(rn)
	}

	rabbitConn, err := amqp.Dial(rn.amqpUrl)
	if err != nil {
		return nil, err
	}

	rabbitCh, err := rabbitConn.Channel()
	if err != nil {
		return nil, err
	}

	rn.conn = rabbitConn
	rn.channel = rabbitCh

	return rn, nil
}
