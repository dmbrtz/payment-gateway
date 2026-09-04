package metrics

import "github.com/prometheus/client_golang/prometheus"

var PaymentsCreated = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "payments_created_total",
	Help: "The total number of payments created",
})

var PaymentProcessingDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name: "payments_processing_duration_seconds",
	Buckets: []float64{
		0.01,
		0.025,
		0.1,
		0.25,
		0.5,
		1,
		2.5,
		5,
	},
	Help: "The duration of payments processing in seconds",
})

var PaymentsFailed = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "payments_failed_total",
	Help: "The total number of payments failed",
})

func init() {
	prometheus.MustRegister(PaymentsCreated)
	prometheus.MustRegister(PaymentProcessingDuration)
	prometheus.MustRegister(PaymentsFailed)
}
