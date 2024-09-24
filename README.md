# restaurants-orders
Consumes restaurant orders

## Receiving orders
Orders are received from a kafka topic and stored into a database.
From there they can be looked up by the restaurants.

When an order is successfully received a notification is sent via RabbitMQ
to the restaurant.

## Transactional processing
When an order is received the consumer begins a transaction.
If the order is successfully stored and the notification is sent then the
transaction is commited. Otherwise the transaction is rolled back.

## Notifications
The service sends notifications to tell the parties involed in the order
what has happened.

The submitting user will be notified when:
- the order is submitted successfully or
- the order is not submitted successfully.

The receiving restaurant will be notified when:
- the order is submitted successfully.
