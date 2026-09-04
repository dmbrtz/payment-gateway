package repository

import (
	"context"
	"errors"
	"payment-gateway/internal/readmodel"
)

type PaymentViewRepository interface {
	GetByID(
		ctx context.Context,
		id string,
	) (*readmodel.PaymentView, error)
}

var ErrPaymentViewNotFound = errors.New("payment view not found")
