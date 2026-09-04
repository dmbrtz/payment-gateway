package commands

import "time"

type ProviderCallbackCommand struct {
	CommandID  string      `json:"command_id"`
	PaymentID  string      `json:"payment_id"`
	Status     string      `json:"status"`
	ReceivedAt time.Time   `json:"received_at"`
	Type       CommandType `json:"type"`
}
