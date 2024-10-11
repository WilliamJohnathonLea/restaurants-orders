package notifier

import amqp "github.com/rabbitmq/amqp091-go"

type RabbitNotifer struct {
	channel *amqp.Channel
}

func (rn *RabbitNotifer) Close() error {
	return rn.channel.Close()
}

func (rn *RabbitNotifer) Notify(notification RabbitNotification) error {
	// The exchange, queue and bind are assumed to be in place
	err := rn.channel.Publish(
		notification.Exchange,
		notification.RoutingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        notification.Body,
		},
	)

	return err
}

func NewRabbitNotifier(conn *amqp.Connection) (*RabbitNotifer, error) {
	rn := &RabbitNotifer{}

	rabbitCh, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	rn.channel = rabbitCh

	return rn, nil
}
