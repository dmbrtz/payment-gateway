package events

import (
	"payment-gateway/internal/payment"
	"time"
)

type PaymentCreatedEvent struct {
	EventID    string         `json:"event_id"`
	PaymentID  string         `json:"payment_id"`
	ClientID   string         `json:"client_id"`
	Amount     int64          `json:"amount"`
	Currency   string         `json:"currency"`
	Provider   string         `json:"provider"`
	OccurredAt time.Time      `json:"occurred_at"`
	Status     payment.Status `json:"status"`
	Version    int64          `json:"version"`
	Type       EventType      `json:"type"`
}

type PaymentStatusChangedEvent struct {
	EventID    string         `json:"event_id"`
	PaymentID  string         `json:"payment_id"`
	Status     payment.Status `json:"status"`
	Version    int64          `json:"version"`
	OccurredAt time.Time      `json:"occurred_at"`
	Type       EventType      `json:"type"`
}

type EventType string

const (
	EventTypePaymentCreated       EventType = "payment.created"
	EventTypePaymentStatusChanged EventType = "payment.status_changed"
)
