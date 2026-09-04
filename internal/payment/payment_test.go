package payment

import "testing"

func TestNewPayment(t *testing.T) {
	p, err := NewPayment(
		"payment-1",
		"client-1",
		1000,
		"USD",
		"mock",
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if p.Status != StatusNew {
		t.Errorf("payment status: got %v, want %v", p.Status, StatusNew)
	}

	if p.Currency != "USD" {
		t.Errorf("payment currency: got %v, want %v", p.Currency, "USD")
	}

	if p.Amount != 1000 {
		t.Errorf("payment amount incorrect : got %v, want %v", p.Amount, 1000)
	}

	if p.Version != 1 {
		t.Errorf("payment version: got %v, want %v", p.Version, 1)
	}

	if p.CreatedAt.IsZero() {
		t.Error("payment CreatedAt must not be zero")
	}

	if p.UpdatedAt.IsZero() {
		t.Error("payment UpdatedAt must not be zero")
	}

	if !p.CreatedAt.Equal(p.UpdatedAt) {
		t.Errorf("payment CreatedAt and UpdatedAt must be equal on creation procedure: got %v, want %v",
			p.CreatedAt,
			p.UpdatedAt)
	}
}

func TestPaymentChangeStatus(t *testing.T) {
	tests := []struct {
		name string
		from Status
		to   Status
	}{
		{
			name: "new to processing",
			from: StatusNew,
			to:   StatusProcessing,
		},
		{
			name: "processing to authorized",
			from: StatusProcessing,
			to:   StatusAuthorized,
		},
		{
			name: "processing to declined",
			from: StatusProcessing,
			to:   StatusDeclined,
		},
		{
			name: "processing to failed",
			from: StatusProcessing,
			to:   StatusFailed,
		},
		{
			name: "authorized to captured",
			from: StatusAuthorized,
			to:   StatusCaptured,
		},
		{
			name: "authorized to cancelled",
			from: StatusAuthorized,
			to:   StatusCanceled,
		},
		{
			name: "captured to refunded",
			from: StatusCaptured,
			to:   StatusRefunded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Payment{
				Status:  tt.from,
				Version: 1,
			}

			err := p.ChangeStatus(tt.to)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if p.Status != tt.to {
				t.Errorf("status: got %v, want %v", p.Status, tt.to)
			}

			if p.Version != 2 {
				t.Errorf("version: got %v, want %v", p.Version, 2)
			}

			if p.UpdatedAt.IsZero() {
				t.Error("payment UpdatedAt must not be zero")
			}
		},
		)
	}

}
