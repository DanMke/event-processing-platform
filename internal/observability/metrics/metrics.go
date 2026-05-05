package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds all Prometheus instruments for the processor.
type Metrics struct {
	EventsProcessedTotal  prometheus.Counter
	EventsFailedTotal     prometheus.Counter
	EventsInvalidTotal    prometheus.Counter
	EventsDuplicatedTotal prometheus.Counter
	EventsSentToDLQTotal  prometheus.Counter
	ProcessingDuration    prometheus.Histogram
}

// New registers and returns the processor metrics using the provided registerer.
// Pass prometheus.DefaultRegisterer for production or a fresh prometheus.NewRegistry()
// for isolated tests.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		EventsProcessedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "events_processed_total",
			Help: "Total events successfully validated and persisted.",
		}),
		EventsFailedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "events_failed_total",
			Help: "Total events that failed due to transient errors (e.g. database unavailable) after retries.",
		}),
		EventsInvalidTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "events_invalid_total",
			Help: "Total events rejected because of invalid JSON, envelope, or payload schema.",
		}),
		EventsDuplicatedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "events_duplicated_total",
			Help: "Total duplicate events silently ignored (idempotency).",
		}),
		EventsSentToDLQTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "events_sent_to_dlq_total",
			Help: "Total events forwarded to the dead-letter queue.",
		}),
		ProcessingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "event_processing_duration_seconds",
			Help:    "End-to-end processing time per event (unmarshal → validate → persist).",
			Buckets: prometheus.DefBuckets,
		}),
	}

	reg.MustRegister(
		m.EventsProcessedTotal,
		m.EventsFailedTotal,
		m.EventsInvalidTotal,
		m.EventsDuplicatedTotal,
		m.EventsSentToDLQTotal,
		m.ProcessingDuration,
	)

	return m
}
