package callback

import (
	"encoding/json"
	"fmt"
	"net/http"
	"payment-gateway/internal/commands"
	"payment-gateway/internal/publisher"
	"time"
)

type Handler struct {
	publisher publisher.CommandPublisher
}

func NewHandler(p publisher.CommandPublisher) *Handler {
	return &Handler{
		publisher: p,
	}
}

func (h *Handler) HandlePaymentCallback(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PaymentCallbackRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.PaymentID == "" || req.Status == "" || req.ProviderEventID == "" {
		http.Error(w, "payment_id and status are required", http.StatusBadRequest)
		return
	}

	cmd := commands.ProviderCallbackCommand{
		Type:            commands.CommandTypeProviderCallback,
		CommandID:       fmt.Sprintf("command-%d", time.Now().UnixNano()),
		PaymentID:       req.PaymentID,
		ProviderEventID: req.ProviderEventID,
		Status:          req.Status,
		ReceivedAt:      time.Now().UTC(),
	}

	if err := h.publisher.PublishProviderCallback(
		r.Context(), cmd); err != nil {
		http.Error(w, "callback service unavailable", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
