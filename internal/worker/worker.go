package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"payment-gateway/internal/commands"
	"payment-gateway/internal/database"
	"payment-gateway/internal/events"
	"payment-gateway/internal/outbox"
	"payment-gateway/internal/payment"
	"payment-gateway/internal/provider"
	"payment-gateway/internal/repository"
	"time"

	"go.opentelemetry.io/otel/trace"
)

var (
	ErrIdempotencyKeyConflict        = errors.New("idempotency key conflict")
	ErrInvalidProviderCallbackStatus = errors.New("unknown provider callback status")
)

type Worker struct {
	repository         repository.PaymentRepository
	tracer             trace.Tracer
	provider           provider.PaymentProvider
	outboxRepository   outbox.OutboxRepository
	transactionManager database.TransactionManager
}

func NewWorker(
	r repository.PaymentRepository,
	t trace.Tracer,
	prov provider.PaymentProvider,
	obr outbox.OutboxRepository,
	tm database.TransactionManager,
) *Worker {
	return &Worker{
		repository:         r,
		tracer:             t,
		provider:           prov,
		outboxRepository:   obr,
		transactionManager: tm,
	}
}

func (w *Worker) HandleCreatePayment(
	ctx context.Context,
	cmd commands.CreatePaymentCommand,
) error {
	existingPayment, err := w.repository.GetByIdempotencyKey(ctx, cmd.IdempotencyKey)
	if err == nil {
		if !isSamePaymentRequest(existingPayment, cmd) {
			return ErrIdempotencyKeyConflict
		}

		return nil
	}

	if !errors.Is(err, repository.ErrPaymentNotFound) {
		return err
	}

	createdPayment, err := payment.NewPayment(
		cmd.PaymentID,
		cmd.ClientID,
		cmd.IdempotencyKey,
		cmd.Amount,
		cmd.Currency,
		cmd.Provider,
	)
	if err != nil {
		return err
	}

	paymentCreatedEvent := events.PaymentCreatedEvent{
		EventID:    fmt.Sprintf("event-%d", time.Now().UnixNano()),
		PaymentID:  createdPayment.ID,
		ClientID:   createdPayment.ClientID,
		Amount:     createdPayment.Amount,
		Currency:   createdPayment.Currency,
		Provider:   createdPayment.Provider,
		Status:     createdPayment.Status,
		Version:    createdPayment.Version,
		OccurredAt: time.Now().UTC(),
		Type:       events.EventTypePaymentCreated,
	}

	payload, err := json.Marshal(paymentCreatedEvent)
	if err != nil {
		return fmt.Errorf(
			"marshal payment created event error: %w", err,
		)
	}

	outboxEvent := outbox.NewOutboxEvent(
		paymentCreatedEvent.PaymentID,
		string(paymentCreatedEvent.Type),
		payload,
	)

	saveCtx, saveSpan := w.tracer.Start(ctx, "postgres-save")

	tx, err := w.transactionManager.Begin(saveCtx)
	if err != nil {
		saveSpan.End()
		return err
	}

	defer tx.Rollback(saveCtx)

	err = w.repository.SaveTx(
		saveCtx,
		tx,
		createdPayment,
	)

	if errors.Is(err, repository.ErrIdempotencyConflict) {
		_ = tx.Rollback(saveCtx)
		saveSpan.End()

		existingPayment, getErr := w.repository.GetByIdempotencyKey(ctx, cmd.IdempotencyKey)

		if getErr != nil {
			return getErr
		}

		if !isSamePaymentRequest(existingPayment, cmd) {
			return ErrIdempotencyKeyConflict
		}

		return nil

	}
	if err != nil {
		saveSpan.End()
		return err
	}

	err = w.outboxRepository.SaveTx(
		saveCtx,
		tx,
		outboxEvent,
	)
	if err != nil {
		saveSpan.End()
		return err
	}

	err = tx.Commit(saveCtx)
	saveSpan.End()
	if err != nil {
		return err
	}

	err = w.updatePaymentStatus(
		ctx,
		&createdPayment,
		payment.StatusProcessing,
	)
	if err != nil {
		return err
	}

	providerRequest := provider.PaymentRequest{
		PaymentID: createdPayment.ID,
		Currency:  createdPayment.Currency,
		Amount:    createdPayment.Amount,
	}

	err = w.provider.Process(
		ctx,
		providerRequest,
	)

	if err != nil {
		err = w.updatePaymentStatus(
			ctx,
			&createdPayment,
			payment.StatusFailed,
		)
		if err != nil {
			return err
		}
		return nil
	}

	return nil
}

func (w *Worker) HandleProviderCallback(
	ctx context.Context,
	cmd commands.ProviderCallbackCommand,
) error {
	p, err := w.repository.GetByID(ctx, cmd.PaymentID)
	if err != nil {
		return err
	}

	var newStatus payment.Status

	switch cmd.Status {
	case "authorized":
		newStatus = payment.StatusAuthorized
	case "declined":
		newStatus = payment.StatusDeclined
	case "failed":
		newStatus = payment.StatusFailed
	default:
		return fmt.Errorf("%w: %s", ErrInvalidProviderCallbackStatus,
			cmd.Status,
		)
	}

	if p.Status != newStatus {
		err = w.updatePaymentStatus(
			ctx,
			&p,
			newStatus,
		)
		if err != nil {
			return err
		}
	}

	err = w.repository.SaveProcessedProviderCallback(
		ctx,
		cmd.ProviderEventID,
		cmd.PaymentID,
		cmd.ReceivedAt,
	)

	if errors.Is(err, repository.ErrProviderCallbackAlreadyProcessed) {
		return nil
	}

	if err != nil {
		return err
	}

	return nil
}

func (w *Worker) updatePaymentStatus(
	ctx context.Context,
	p *payment.Payment,
	status payment.Status,
) error {
	err := p.ChangeStatus(status)
	if err != nil {
		return err
	}

	event := events.PaymentStatusChangedEvent{
		EventID:    fmt.Sprintf("event-%d", time.Now().UnixNano()),
		PaymentID:  p.ID,
		Status:     p.Status,
		Version:    p.Version,
		OccurredAt: time.Now().UTC(),
		Type:       events.EventTypePaymentStatusChanged,
	}

	payload, err := json.Marshal(event)

	if err != nil {
		return fmt.Errorf("failed to marshal status changed event: %w", err)
	}

	outboxEvent := outbox.NewOutboxEvent(
		event.PaymentID,
		string(event.Type),
		payload,
	)

	tx, err := w.transactionManager.Begin(ctx)

	if err != nil {
		return err
	}

	defer tx.Rollback(ctx)

	err = w.repository.UpdateTx(ctx, tx, *p)
	if err != nil {
		return err
	}

	err = w.outboxRepository.SaveTx(
		ctx,
		tx,
		outboxEvent,
	)

	if err != nil {
		return err
	}

	err = tx.Commit(ctx)

	if err != nil {
		return err
	}

	return nil
}

func isSamePaymentRequest(
	p payment.Payment,
	cmd commands.CreatePaymentCommand,
) bool {
	if p.ClientID != cmd.ClientID ||
		p.Amount != cmd.Amount ||
		p.Currency != cmd.Currency ||
		p.Provider != cmd.Provider {
		return false
	}

	return true
}
