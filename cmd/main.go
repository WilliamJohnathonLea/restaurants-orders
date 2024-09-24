package main

import (
	"fmt"
	"log"
	"os"

	"github.com/IBM/sarama"
	"github.com/WilliamJohnathonLea/restaurants-orders/consumer"
	"github.com/WilliamJohnathonLea/restaurants-orders/notifier"
	"github.com/gocraft/dbr/v2"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("failed to load environment")
	}

	dbUsername := os.Getenv("DB_USERNAME")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbHost := os.Getenv("DB_HOST")

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	ordersIngestTopic := os.Getenv("ORDERS_INGESTION_TOPIC")

	amqpUsername := os.Getenv("AMQP_USERNAME")
	amqpPassword := os.Getenv("AMQP_PASSWORD")
	amqpHost := os.Getenv("AMQP_HOST")

	dbUrl := fmt.Sprintf(
		"postgres://%s:%s@%s/restaurants?sslmode=disable",
		dbUsername,
		dbPassword,
		dbHost,
	)
	bootstrapServers := []string{kafkaBroker}
	topics := []string{ordersIngestTopic}

	// Open DB connection
	conn, err := dbr.Open("postgres", dbUrl, nil)
	if err != nil {
		log.Fatalf("error opening db connection %+v", err)
	}
	sess := conn.NewSession(nil)
	defer sess.Close()

	// Set up AMQP
	amqpUrl := fmt.Sprintf(
		"amqp://%s:%s@%s/",
		amqpUsername,
		amqpPassword,
		amqpHost,
	)
	rn, err := notifier.NewRabbitNotifier(
		notifier.WithURL(amqpUrl),
	)
	if err != nil {
		log.Fatal("failed to connect to rabbitmq")
	}
	defer rn.Close()

	// Set up Order Consumer
	kafkaConf := sarama.NewConfig()
	consumer, err := consumer.NewKafkaConsumer(
		kafkaConf,
		sess,
		rn,
		bootstrapServers,
		topics,
	)
	if err != nil {
		log.Fatal("failed to initialise kafka consumer")
	}
	defer consumer.Close()

	consumer.Consume()

}
