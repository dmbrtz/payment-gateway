package provider

import (
	"context"
	"errors"
)

type MockProvider struct {
	mode string
}

func NewMockProvider(mode string) *MockProvider {
	return &MockProvider{
		mode: mode,
	}
}

func (p *MockProvider) Process(
	ctx context.Context,
	req PaymentRequest,
) error {
	switch p.mode {
	case "success":
		return nil
	case "decline":
		return nil
	case "error":
		return errors.New("mock provider error")
	default:
		return errors.New("unknown mock provider mode")
	}
}
