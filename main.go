package main

import (
	"log"

	"github.com/IBM/sarama"
	"github.com/gocraft/dbr/v2"
)

func main() {
	dbUrl := "postgres://postgres:postgres@localhost:5432/restaurants?sslmode=disable"
	bootstrapServers := []string{"localhost:9092"}
	topics := []string{"orders"}

	// Open DB connection
	conn, err := dbr.Open("postgres", dbUrl, nil)
	if err != nil {
		log.Fatalf("error opening db connection %+v", err)
	}
	sess := conn.NewSession(nil)
	defer sess.Close()

	// Set up Order Consumer
	kafkaConf := sarama.NewConfig()
	consumer, err := NewKafkaConsumer(kafkaConf, sess, bootstrapServers, topics)
	if err != nil {
		log.Fatal("failed to initialise kafka consumer")
	}
	defer consumer.Close()

	consumer.Consume()

}
