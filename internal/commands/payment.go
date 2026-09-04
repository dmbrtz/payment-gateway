package commands

import "time"

type CreatePaymentCommand struct {
	CommandID   string      `json:"command_id"`
	PaymentID   string      `json:"payment_id"`
	ClientID    string      `json:"client_id"`
	Amount      int64       `json:"amount"`
	Currency    string      `json:"currency"`
	Provider    string      `json:"provider"`
	RequestedAt time.Time   `json:"requested_at"`
	Type        CommandType `json:"type"`
}

type CommandType string

const (
	CommandTypeCreatePayment    CommandType = "payment.create"
	CommandTypeProviderCallback CommandType = "payment.callback"
)
