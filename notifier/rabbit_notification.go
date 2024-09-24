package notifier

type RabbitNotification struct {
	Exchange   string
	RoutingKey string
	Body       []byte
}
