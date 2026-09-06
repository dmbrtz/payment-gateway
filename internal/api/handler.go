package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"payment-gateway/internal/commands"
	"payment-gateway/internal/publisher"
	"payment-gateway/internal/repository"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

func (h *Handler) CreatePaymentHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx, span := h.tracer.Start(r.Context(), "POST /payments")
	defer span.End()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req CreatePaymentRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	idempotencyKey := r.Header.Get("Idempotency-Key")

	if idempotencyKey == "" {
		http.Error(w, "idempotency header missing", http.StatusBadRequest)
		return
	}
	paymentID := "payment-" + uuid.NewSHA1(
		uuid.NameSpaceOID,
		[]byte("payment:"+idempotencyKey),
	).String()
	commandID := "command-" + uuid.NewSHA1(
		uuid.NameSpaceOID,
		[]byte("create-payment:"+idempotencyKey),
	).String()
	requestedAt := time.Now().UTC()

	cmd := commands.CreatePaymentCommand{
		CommandID:      commandID,
		PaymentID:      paymentID,
		ClientID:       req.ClientID,
		IdempotencyKey: idempotencyKey,
		Amount:         req.Amount,
		Currency:       req.Currency,
		Provider:       req.Provider,
		RequestedAt:    requestedAt,
		Type:           commands.CommandTypeCreatePayment,
	}

	err = h.publisher.PublishCreatePayment(ctx, cmd)
	if err != nil {
		fmt.Println("publish error", err)

		http.Error(
			w,
			"command service unavailable", http.StatusServiceUnavailable,
		)
		return
	}
	//p, err := payment.NewPayment(
	//	paymentID,
	//	req.ClientID,
	//	req.Amount,
	//	req.Currency,
	//	req.Provider,
	//)
	//if err != nil {
	//	http.Error(w, err.Error(), http.StatusBadRequest)
	//	return
	//}
	response := CreatePaymentResponse{
		PaymentID: cmd.PaymentID,
		//Status:    p.Status,
		//CreatedAt: p.CreatedAt,
		CommandID: cmd.CommandID,
	}

	// заголовки и статус устанавливаем ДО записи тела запроса
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)

	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		return
	}
}

func (h *Handler) GetPaymentHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/payments/")
	if id == "" {
		http.Error(w, "payment id is required", http.StatusBadRequest)
		return
	}
	p, err := h.viewRepo.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrPaymentViewNotFound) {
			http.Error(w, "payment not found", http.StatusNotFound)
			return
		}
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	response := PaymentViewResponse{
		PaymentID: p.ID,
		ClientID:  p.ClientID,
		Amount:    p.Amount,
		Currency:  p.Currency,
		Status:    p.Status,
		Provider:  p.Provider,
		CreatedAt: p.CreatedAt,
		UpdatedAt: p.UpdatedAt,
		Version:   p.Version,
	}
	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		return
	}

}

type Handler struct {
	publisher publisher.CommandPublisher
	tracer    trace.Tracer
	viewRepo  repository.PaymentViewRepository
}

func NewHandler(p publisher.CommandPublisher, tracer trace.Tracer, viewRepo repository.PaymentViewRepository) *Handler {
	return &Handler{
		publisher: p,
		tracer:    tracer,
		viewRepo:  viewRepo,
	}
}
