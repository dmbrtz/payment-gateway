package worker

import (
	"context"
	"errors"
	"fmt"
	"payment-gateway/internal/commands"
	"payment-gateway/internal/events"
	"payment-gateway/internal/payment"
	"payment-gateway/internal/provider"
	"payment-gateway/internal/publisher"
	"payment-gateway/internal/repository"
	"time"

	"go.opentelemetry.io/otel/trace"
)

var ErrIdempotencyKeyConflict = errors.New("idempotency key conflict")

type Worker struct {
	repository repository.PaymentRepository
	publisher  publisher.EventPublisher
	tracer     trace.Tracer
	provider   provider.PaymentProvider
}

func NewWorker(
	r repository.PaymentRepository,
	p publisher.EventPublisher,
	t trace.Tracer,
	prov provider.PaymentProvider,
) *Worker {
	return &Worker{
		repository: r,
		publisher:  p,
		tracer:     t,
		provider:   prov,
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

	saveCtx, saveSpan := w.tracer.Start(ctx, "postgres-save")

	err = w.repository.Save(
		saveCtx,
		createdPayment,
	)

	saveSpan.End()

	if errors.Is(err, repository.ErrIdempotencyConflict) {
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

	publishCtx, publishSpan := w.tracer.Start(ctx, "kafka-publish-payment-created")

	err = w.publisher.PublishPaymentCreated(
		publishCtx,
		paymentCreatedEvent,
	)

	publishSpan.End()

	if err != nil {
		return err
	}

	err = createdPayment.ChangeStatus(payment.StatusProcessing)

	if err != nil {
		return err
	}

	err = w.repository.Update(ctx, createdPayment)

	if err != nil {
		return err
	}

	statusChangedEvent := events.PaymentStatusChangedEvent{
		EventID:    fmt.Sprintf("event-%d", time.Now().UnixNano()),
		PaymentID:  createdPayment.ID,
		Status:     createdPayment.Status,
		Version:    createdPayment.Version,
		OccurredAt: time.Now().UTC(),
		Type:       events.EventTypePaymentStatusChanged,
	}

	err = w.publisher.PublishPaymentStatusChanged(
		ctx,
		statusChangedEvent,
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
		return fmt.Errorf("unknown provider callback status: %s", cmd.Status)
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

	err = w.repository.Update(ctx, *p)
	if err != nil {
		return err
	}

	err = w.publisher.PublishPaymentStatusChanged(ctx, event)
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
