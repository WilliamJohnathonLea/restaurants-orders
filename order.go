package main

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

func InsertNewOrder(s *dbr.Session, o Order) error {
	// Adjust timezones to UTC
	o.CreatedAt = o.CreatedAt.UTC()
	if o.CompletedAt != nil {
		*o.CompletedAt = o.CompletedAt.UTC()
	}

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

func GetOrderByID(s *dbr.Session, orderID string) (Order, error) {
	var order Order
	var items []LineItem

	err := s.Select("id", "restaurant_id", "created_at", "completed_at").
		From("orders").
		Where("id = ?", orderID).
		LoadOne(&order)

	if err != nil {
		return order, err
	}

	// adjust timezone to UTC
	order.CreatedAt = order.CreatedAt.UTC()
	if order.CompletedAt != nil {
		*order.CompletedAt = order.CompletedAt.UTC()
	}

	_, err = s.Select("id", "order_id", "item_id", "name", "price", "quantity").
		From("line_items").
		Where("order_id = ?", order.ID).
		Load(&items)

	order.Items = items

	return order, err
}
