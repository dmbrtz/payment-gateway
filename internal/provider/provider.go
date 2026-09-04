package provider

import "context"

type PaymentProvider interface {
	Process(
		ctx context.Context,
		req PaymentRequest,
	) error
}

type PaymentRequest struct {
	PaymentID string
	Amount    int64
	Currency  string
}
