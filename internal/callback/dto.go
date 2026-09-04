package callback

type PaymentCallbackRequest struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"`
}
