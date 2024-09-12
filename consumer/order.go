package consumer

import (
	"time"

	"github.com/gocraft/dbr/v2"
)

type LineItem struct {
	// ID is the unique ID for this line item entry in the database
	ID       string  `json:"id,omitempty"`
	// OrderID is the ID of the Order to which this LineItem belongs
	OrderID  string  `json:"orderId,omitempty"`
	// ItemID refers to the ID of the MenuItem
	ItemID   string  `json:"itemId"`
	// Name refers to the Name of the MenuItem
	Name     string  `json:"name"`
	Price    float32 `json:"price"`
	Quantity uint    `json:"quantity"`
}

type Order struct {
	ID           string     `json:"id,omitempty"`
	RestaurantID string     `json:"restaurantId"`
	Items        []LineItem `json:"items"`
	CreatedAt    time.Time  `json:"createdAt"`
	CompletedAt  *time.Time `json:"completedAt,omitempty"`
}

// Inserts a new Order and its LineItems to the database.
//
// Use a transaction around this function so that if an error occurs,
// the database doesn't contain Orders with no LineItems.
func InsertNewOrder(s *dbr.Session, o Order) error {
	// Adjust timezones to UTC
	o.CreatedAt = o.CreatedAt.UTC()

	// NOTE: DO NOT write the CompletedAt field to the db here.
	// New orders MUST NOT be submitted as completed.

	// Insert Order
	_, err := s.InsertInto("orders").
		Columns("id", "restaurant_id", "created_at").
		Values(o.ID, o.RestaurantID, o.CreatedAt).
		Exec()

	if err != nil {
		return err
	}

	// Insert Items of the Order
	baseQuery := s.InsertInto("line_items").
		Columns("id", "order_id", "item_id", "name", "price", "quantity")

	for _, item := range o.Items {
		baseQuery.Record(item)
	}

	_, err = baseQuery.Exec()

	return err
}
