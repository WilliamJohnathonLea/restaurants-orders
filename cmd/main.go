package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/WilliamJohnathonLea/restaurants-orders/consumer"
	"github.com/WilliamJohnathonLea/restaurants-orders/notifier"
	"github.com/gocraft/dbr/v2"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("failed to load environment")
	}

	dbUsername := os.Getenv("DB_USERNAME")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbHost := os.Getenv("DB_HOST")

	ordersIngestQueue := os.Getenv("ORDERS_INGESTION_QUEUE")

	amqpUsername := os.Getenv("AMQP_USERNAME")
	amqpPassword := os.Getenv("AMQP_PASSWORD")
	amqpHost := os.Getenv("AMQP_HOST")

	dbUrl := fmt.Sprintf(
		"postgres://%s:%s@%s/restaurants?sslmode=disable",
		dbUsername,
		dbPassword,
		dbHost,
	)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

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
	amqpConn, err := amqp.Dial(amqpUrl)
	if err != nil {
		log.Fatal("failed to connect to rabbitmq")
	}
	defer amqpConn.Close()

	// Set up Notifier
	rn, err := notifier.NewRabbitNotifier(amqpConn)
	if err != nil {
		log.Fatal("failed to create rabbit notifier")
	}
	defer rn.Close()

	// Set up Order Consumer
	rc, err := consumer.NewRabbitConsumer(amqpConn, sess, ordersIngestQueue)
	if err != nil {
		log.Fatal("failed to create rabbit consumer")
	}
	defer rc.Close()

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		// cancel is called here to handle the case where the Consumer
		// stops due to an error.
		defer cancel()
		err := rc.Consume(ctx)
		if err != nil {
			log.Printf("consumer returned an error %s", err.Error())
		}
	}()

	select {
	case <-sigs:
		cancel()
		log.Println("application ternminated by signal")
	case <-ctx.Done():
		log.Println("consumer context cancelled")
	}

	log.Println("waiting for consumer process remaining messages")
	wg.Wait()
	log.Println("all done")

}
