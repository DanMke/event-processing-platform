package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	// EventsReceivedTotal counts every call to Handle before any processing.
	// No labels: we may not be able to parse tenant/type if JSON is malformed.
	EventsReceivedTotal prometheus.Counter

	// Labelled counters — available only after a successful unmarshal.

	// EventsProcessedTotal counts events successfully validated and persisted.
	// Labels: tenant_id, event_type, producer
	EventsProcessedTotal *prometheus.CounterVec

	// EventsInvalidTotal counts events rejected by validation.
	// Labels: reason (unmarshal_error | envelope_error | payload_error)
	EventsInvalidTotal *prometheus.CounterVec

	// EventsDuplicatedTotal counts duplicate events silently ignored.
	// Labels: tenant_id, event_type
	EventsDuplicatedTotal *prometheus.CounterVec

	// EventsFailedTotal counts events that failed after all retries.
	// Labels: tenant_id, event_type
	EventsFailedTotal *prometheus.CounterVec

	// EventsSentToDLQTotal counts events forwarded to the dead-letter queue.
	// Labels: reason (unmarshal_error | envelope_error | payload_error)
	EventsSentToDLQTotal *prometheus.CounterVec

	// EventsInFlight is a gauge of messages currently being processed.
	EventsInFlight prometheus.Gauge

	// ProcessingDuration measures end-to-end handler time per event_type.
	// Labels: event_type
	ProcessingDuration *prometheus.HistogramVec

	// MessageAgeSeconds measures the delay between OccurredAt and processing time.
	// High values indicate consumer lag or slow producers.
	// Labels: event_type
	MessageAgeSeconds *prometheus.HistogramVec
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		EventsReceivedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "events_received_total",
			Help: "Total events received by the handler before any processing.",
		}),

		EventsProcessedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "events_processed_total",
			Help: "Total events successfully validated and persisted.",
		}, []string{"tenant_id", "event_type", "producer"}),

		EventsInvalidTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "events_invalid_total",
			Help: "Total events rejected by validation.",
		}, []string{"reason"}),

		EventsDuplicatedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "events_duplicated_total",
			Help: "Total duplicate events silently ignored (idempotency).",
		}, []string{"tenant_id", "event_type"}),

		EventsFailedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "events_failed_total",
			Help: "Total events that failed due to transient errors after retries.",
		}, []string{"tenant_id", "event_type"}),

		EventsSentToDLQTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "events_sent_to_dlq_total",
			Help: "Total events forwarded to the dead-letter queue.",
		}, []string{"reason"}),

		EventsInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "events_in_flight",
			Help: "Number of events currently being processed by the handler.",
		}),

		ProcessingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "event_processing_duration_seconds",
			Help:    "End-to-end handler time per event (unmarshal → validate → persist).",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}, []string{"event_type"}),

		MessageAgeSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "event_message_age_seconds",
			Help:    "Delay between OccurredAt and processing time. High values indicate consumer lag.",
			Buckets: []float64{.1, .5, 1, 5, 10, 30, 60, 300},
		}, []string{"event_type"}),
	}

	reg.MustRegister(
		m.EventsReceivedTotal,
		m.EventsProcessedTotal,
		m.EventsInvalidTotal,
		m.EventsDuplicatedTotal,
		m.EventsFailedTotal,
		m.EventsSentToDLQTotal,
		m.EventsInFlight,
		m.ProcessingDuration,
		m.MessageAgeSeconds,
	)

	return m
}
