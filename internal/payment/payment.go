package payment

import (
	"errors"
	"time"
)

type Status string

const (
	StatusNew        Status = "NEW"
	StatusProcessing Status = "PROCESSING"
	StatusAuthorized Status = "AUTHORIZED"
	StatusDeclined   Status = "DECLINED"
	StatusCaptured   Status = "CAPTURED"
	StatusFailed     Status = "FAILED"
	StatusCanceled   Status = "CANCELED"
	StatusRefunded   Status = "REFUNDED"
)

type Payment struct {
	ID        string
	ClientID  string
	Amount    int64
	Currency  string
	Status    Status
	Provider  string
	CreatedAt time.Time
	UpdatedAt time.Time
	Version   int64
}

func NewPayment(
	id string,
	clientID string,
	amount int64,
	currency string,
	provider string,
) (Payment, error) {
	if amount <= 0 {
		return Payment{}, errors.New("amount must be greater than zero")
	}
	if id == "" {
		return Payment{}, errors.New("invalid payment id")
	}
	if currency == "" {
		return Payment{}, errors.New("invalid payment currency")
	}
	if provider == "" {
		return Payment{}, errors.New("invalid payment provider")
	}
	if clientID == "" {
		return Payment{}, errors.New("invalid payment clientID")
	}

	now := time.Now().UTC()

	payment := Payment{
		ID:        id,
		ClientID:  clientID,
		Amount:    amount,
		Currency:  currency,
		Status:    StatusNew,
		Provider:  provider,
		CreatedAt: now,
		UpdatedAt: now,
		Version:   1,
	}

	return payment, nil
}

//func (p *Payment) ChangeStatus(newStatus Status) error {
//	if canTransition(p.Status, newStatus) {
//		p.Status = newStatus
//		p.UpdatedAt = time.Now().UTC()
//		p.Version += 1
//		return nil
//	}
//	return errors.New("invalid transition order use")
//}

var ErrInvalidStatusTransition = errors.New("invalid status transition")

func (s Status) IsTerminal() bool {
	if s == StatusDeclined || s == StatusFailed || s == StatusCanceled || s == StatusRefunded {
		return true
	}
	return false
}

func (s Status) IsSuccessful() bool {
	if s == StatusCaptured || s == StatusRefunded {
		return true
	}
	return false
}

func (p *Payment) ChangeStatus(newStatus Status) error {
	if !canTransition(p.Status, newStatus) {
		return ErrInvalidStatusTransition
	}

	p.Status = newStatus
	p.UpdatedAt = time.Now().UTC()
	p.Version += 1

	return nil
}

func canTransition(from Status, to Status) bool {
	if from == StatusNew && to == StatusProcessing {
		return true
	}
	if from == StatusProcessing && to == StatusAuthorized {
		return true
	}
	if from == StatusProcessing && to == StatusDeclined {
		return true
	}
	if from == StatusProcessing && to == StatusFailed {
		return true
	}
	if from == StatusAuthorized && to == StatusCaptured {
		return true
	}
	if from == StatusAuthorized && to == StatusCanceled {
		return true
	}
	if from == StatusCaptured && to == StatusRefunded {
		return true
	}
	return false
}
