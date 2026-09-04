package projection

import "time"

type PaymentView struct {
	ID        string
	ClientID  string
	Amount    int64
	Currency  string
	Status    string
	Provider  string
	CreatedAt time.Time
	UpdatedAt time.Time
	Version   int64
}
