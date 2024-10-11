package consumer

import (
	"context"
	"encoding/json"
	"log"

	"github.com/gocraft/dbr/v2"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitConsumer struct {
	channel *amqp.Channel
	queue   string
	db      *dbr.Session
}

func (rc *RabbitConsumer) Close() error {
	return rc.channel.Close()
}

func (rc *RabbitConsumer) Consume(ctx context.Context) error {
	msgs, err := rc.channel.ConsumeWithContext(
		ctx,
		rc.queue, // queue
		"",       // consumer
		true,     // auto-ack
		false,    // exclusive
		false,    // no-local
		false,    // no-wait
		nil,      // args
	)
	if err != nil {
		return err
	}

	for msg := range msgs {
		// Get user ID so we know who to notify of any errors
		// userID, headerExists := msg.Headers["user_id"]
		// if !headerExists {
		// 	continue
		// }
		// userIDStr, ok := userID.(string)
		// if !ok {
		// 	continue
		// }


		tx, err := rc.db.Begin()
		if err != nil {
			continue
		}

		withTx(tx, func() error {
			// 1. Unmarshal the JSON
			var order Order
			err := json.Unmarshal(msg.Body, &order)
			if err != nil {
				log.Printf("error unmarshalling order %s", err.Error())
				return err
			}
			// 2. Apply a new unique ID for the order and its Items
			order.ID = uuid.NewString()
			for idx := range order.Items {
				order.Items[idx].ID = uuid.NewString()
				order.Items[idx].OrderID = order.ID
			}
			// 3. Save the order to the database
			err = InsertNewOrder(tx, order)
			if err != nil {
				log.Printf("error saving order %s", err.Error())
				return err
			}

			return nil
		})
	}

	return nil
}

func NewRabbitConsumer(conn *amqp.Connection, db *dbr.Session, queue string) (Consumer, error) {
	rc := &RabbitConsumer{}

	rabbitCh, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	rc.channel = rabbitCh
	rc.db = db
	rc.queue = queue

	return rc, nil
}
