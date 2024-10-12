package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/WilliamJohnathonLea/restaurants-orders/db"
	"github.com/WilliamJohnathonLea/restaurants-orders/notifier"
	"github.com/gocraft/dbr/v2"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitConsumer struct {
	channel  *amqp.Channel
	queue    string
	db       *dbr.Session
	notifier *notifier.RabbitNotifer
}

func (rc *RabbitConsumer) Close() error {
	return rc.channel.Close()
}

func (rc *RabbitConsumer) Consume(ctx context.Context) error {
	// Make sure ingestion queue is declared
	q, err := rc.channel.QueueDeclare(
		rc.queue, // name
		true,     // durable
		false,    // auto-delete
		false,    // exclusive
		false,    // no-wait
		nil,      // args
	)
	if err != nil {
		return err
	}

	msgs, err := rc.channel.ConsumeWithContext(
		ctx,
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return err
	}

	// Start consuming messages
	for msg := range msgs {
		// Get user ID so we know who to notify of any errors
		userID, ok := getUserHeader(msg.Headers)
		if !ok {
			continue
		}

		tx, err := rc.db.Begin()
		if err != nil {
			continue
		}

		err = db.WithTx(tx, func() error {
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
			// 4. Notify user new order is created
			rc.sendUserNotification(order.UserID, []byte(order.ID))
			// 5. Notify restaurant new order is created
			rc.sendRestaurantNotification(order.RestaurantID, []byte(order.ID))

			return nil
		})
		if err != nil {
			rc.sendUserNotification(userID, []byte("your order could not be processed"))
		}
	}

	return nil
}

func NewRabbitConsumer(conn *amqp.Connection, db *dbr.Session, queue string) (Consumer, error) {
	rc := &RabbitConsumer{}

	rabbitCh, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	notifier, err := notifier.NewRabbitNotifier(conn)
	if err != nil {
		return nil, err
	}

	rc.channel = rabbitCh
	rc.db = db
	rc.queue = queue
	rc.notifier = notifier

	return rc, nil
}

func (rc *RabbitConsumer) sendUserNotification(id string, msg []byte) {
	rc.notifier.Notify(notifier.RabbitNotification{
		Exchange:   "user_notifications",
		RoutingKey: fmt.Sprintf("user_%s", id),
		Body:       msg,
	})
}

func (rc *RabbitConsumer) sendRestaurantNotification(id string, msg []byte) {
	rc.notifier.Notify(notifier.RabbitNotification{
		Exchange:   "restaurant_notifications",
		RoutingKey: fmt.Sprintf("restaurant_%s", id),
		Body:       msg,
	})
}

func getUserHeader(headers amqp.Table) (string, bool) {
	userID, headerExists := headers["user_id"]
	if !headerExists {
		return "", false
	} else {
		userIDStr, ok := userID.(string)
		return userIDStr, ok
	}
}
