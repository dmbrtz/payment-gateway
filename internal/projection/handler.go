package projection

import (
	"context"
	"payment-gateway/internal/events"
)

type Handler struct {
	repository Repository
}

func NewHandler(repository Repository) *Handler {
	return &Handler{
		repository: repository,
	}
}

func (h *Handler) HandlePaymentCreated(
	ctx context.Context,
	event events.PaymentCreatedEvent,
) error {
	view := PaymentView{
		ID:        event.PaymentID,
		ClientID:  event.ClientID,
		Amount:    event.Amount,
		Currency:  event.Currency,
		Status:    string(event.Status),
		Provider:  event.Provider,
		CreatedAt: event.OccurredAt,
		UpdatedAt: event.OccurredAt,
		Version:   event.Version,
	}

	return h.repository.Save(ctx, view)
}

func (h *Handler) HandlePaymentStatusChanged(
	ctx context.Context,
	event events.PaymentStatusChangedEvent,
) error {
	view := PaymentView{
		ID:        event.PaymentID,
		Status:    string(event.Status),
		UpdatedAt: event.OccurredAt,
		Version:   event.Version,
	}
	return h.repository.Update(ctx, view)
}
