package api

import "time"

type CreatePaymentRequest struct {
	ClientID string `json:"client_id"`
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
	Provider string `json:"provider"`
}

type CreatePaymentResponse struct {
	PaymentID string `json:"payment_id"`
	//Status    payment.Status `json:"status"`
	//CreatedAt time.Time `json:"created_at"`
	CommandID string `json:"command_id"`
}

type PaymentViewResponse struct {
	PaymentID string    `json:"payment_id"`
	ClientID  string    `json:"client_id"`
	Amount    int64     `json:"amount"`
	Currency  string    `json:"currency"`
	Status    string    `json:"status"`
	Provider  string    `json:"provider"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Version   int64     `json:"version"`
}
