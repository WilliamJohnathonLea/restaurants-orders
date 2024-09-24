package consumer

import (
	"context"
	"encoding/json"
	"io"
	"log"

	"github.com/IBM/sarama"
	"github.com/WilliamJohnathonLea/restaurants-orders/notifier"
	"github.com/gocraft/dbr/v2"
	"github.com/google/uuid"
)

type Consumer interface {
	io.Closer
	Consume() error
}

type KafkaConsumer struct {
	topics        []string
	consumerGroup sarama.ConsumerGroup
	DB            *dbr.Session
	Notifier      *notifier.RabbitNotifer
}

func NewKafkaConsumer(
	conf *sarama.Config,
	db *dbr.Session,
	notifier *notifier.RabbitNotifer,
	bootstrapServers []string,
	topics []string,
) (Consumer, error) {
	consumer := &KafkaConsumer{
		topics:   topics,
		DB:       db,
		Notifier: notifier,
	}

	grp, err := sarama.NewConsumerGroup(bootstrapServers, "orders", conf)
	if err != nil {
		return nil, err
	}

	consumer.consumerGroup = grp

	return consumer, nil
}

// Implement the Consumer interface
func (k KafkaConsumer) Consume() error {
	return k.consumerGroup.Consume(context.Background(), k.topics, k)
}

// Implement the io.Closer interface
func (k KafkaConsumer) Close() error {
	return k.consumerGroup.Close()
}

func (KafkaConsumer) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (KafkaConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (k KafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("received claim on %s:%d", claim.Topic(), claim.Partition())
	for message := range claim.Messages() {
		log.Println("processing message")

		// Create DB transaction
		tx, err := k.DB.Begin()
		if err != nil {
			session.MarkMessage(message, "")
			continue
		}

		withTx(tx, func() error {
			// 1. Unmarshal the JSON
			var order Order
			err := json.Unmarshal(message.Value, &order)
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
			err = InsertNewOrder(k.DB, order)
			if err != nil {
				log.Printf("error saving order %s", err.Error())
				return err
			}
			// 4. Notify User the order is submitted
			userNotif := notifier.RabbitNotification{
				Exchange:   "user_notifications",
				RoutingKey: order.UserID,
				Body:       []byte(order.ID),
			}
			err = k.Notifier.Notify(userNotif)
			if err != nil {
				log.Printf(
					"error notifying user %s of order %s",
					order.UserID,
					order.ID,
				)
				return err
			}
			// 5. Notify Restaurant about new order
			restNotif := notifier.RabbitNotification{
				Exchange:   "restaurant_notifications",
				RoutingKey: order.RestaurantID,
				Body:       []byte(order.ID),
			}
			err = k.Notifier.Notify(restNotif)
			if err != nil {
				log.Printf(
					"error notifying restaurant %s of order %s",
					order.RestaurantID,
					order.ID,
				)
				return err
			}

			return nil
		})

		session.MarkMessage(message, "")
	}

	return nil
}

func withTx(tx *dbr.Tx, fn func() error) error {
	defer tx.RollbackUnlessCommitted()
	err := fn()
	if err != nil {
		return err
	}
	return tx.Commit()
}
