package callback

type PaymentCallbackRequest struct {
	PaymentID       string `json:"payment_id"`
	ProviderEventID string `json:"provider_event_id"`
	Status          string `json:"status"`
}
