package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/DanMke/event-processing-platform/internal/domain"
	"github.com/DanMke/event-processing-platform/internal/observability/metrics"
	"github.com/DanMke/event-processing-platform/internal/retry"
	"github.com/DanMke/event-processing-platform/internal/validation"
)

type Repository interface {
	Save(ctx context.Context, event domain.Event) error
}

type Validator interface {
	ValidatePayload(event domain.Event) error
}

type DLQPublisher interface {
	Publish(ctx context.Context, original []byte, reason string) error
}

type Option func(*Handler)

// WithMetrics records processing metrics when provided.
func WithMetrics(m *metrics.Metrics) Option {
	return func(h *Handler) { h.metrics = m }
}

type Handler struct {
	repo      Repository
	validator Validator
	dlq       DLQPublisher
	metrics   *metrics.Metrics
}

func NewHandler(repo Repository, validator Validator, dlq DLQPublisher, opts ...Option) *Handler {
	h := &Handler{repo: repo, validator: validator, dlq: dlq}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

func (h *Handler) Handle(ctx context.Context, _ []byte, value []byte) error {
	start := time.Now()
	h.inc(func(m *metrics.Metrics) {
		m.EventsReceivedTotal.Inc()
		m.EventsInFlight.Inc()
	})
	defer h.inc(func(m *metrics.Metrics) { m.EventsInFlight.Dec() })

	var event domain.Event
	if err := json.Unmarshal(value, &event); err != nil {
		slog.Error("unmarshal failed",
			"status", "dlq",
			"error_reason", err.Error(),
		)
		h.inc(func(m *metrics.Metrics) { m.EventsInvalidTotal.WithLabelValues("unmarshal_error").Inc() })
		h.observe(start, "unknown")
		if dlqErr := h.sendToDLQ(ctx, value, fmt.Sprintf("unmarshal: %v", err)); dlqErr != nil {
			return dlqErr
		}
		h.inc(func(m *metrics.Metrics) { m.EventsSentToDLQTotal.WithLabelValues("unmarshal_error").Inc() })
		return nil
	}

	logger := slog.With(
		"event_id", event.EventID,
		"tenant_id", event.TenantID,
		"event_type", event.EventType,
		"schema_version", event.SchemaVersion,
		"producer", event.Producer,
		"trace_id", event.TraceID,
	)

	logger.Info("event received")

	if err := validation.ValidateEnvelope(event); err != nil {
		logger.Warn("invalid envelope",
			"status", "rejected",
			"error_reason", err.Error(),
		)
		h.inc(func(m *metrics.Metrics) { m.EventsInvalidTotal.WithLabelValues("envelope_error").Inc() })
		h.observe(start, event.EventType)
		if dlqErr := h.sendToDLQ(ctx, value, err.Error()); dlqErr != nil {
			return dlqErr
		}
		h.inc(func(m *metrics.Metrics) { m.EventsSentToDLQTotal.WithLabelValues("envelope_error").Inc() })
		return nil
	}

	if err := h.validator.ValidatePayload(event); err != nil {
		logger.Warn("invalid payload",
			"status", "rejected",
			"error_reason", err.Error(),
		)
		h.inc(func(m *metrics.Metrics) { m.EventsInvalidTotal.WithLabelValues("payload_error").Inc() })
		h.observe(start, event.EventType)
		if dlqErr := h.sendToDLQ(ctx, value, err.Error()); dlqErr != nil {
			return dlqErr
		}
		h.inc(func(m *metrics.Metrics) { m.EventsSentToDLQTotal.WithLabelValues("payload_error").Inc() })
		return nil
	}

	var isDuplicate bool
	saveErr := retry.Do(ctx, func() error {
		err := h.repo.Save(ctx, event)
		if errors.Is(err, domain.ErrDuplicateEvent) {
			isDuplicate = true
			return nil
		}
		return err
	})

	h.observe(start, event.EventType)

	if isDuplicate {
		logger.Info("duplicate event ignored", "status", "skipped")
		h.inc(func(m *metrics.Metrics) {
			m.EventsDuplicatedTotal.WithLabelValues(event.TenantID, event.EventType).Inc()
		})
		return nil
	}

	if saveErr != nil {
		logger.Error("save failed after retries",
			"status", "failed",
			"error_reason", saveErr.Error(),
		)
		h.inc(func(m *metrics.Metrics) {
			m.EventsFailedTotal.WithLabelValues(event.TenantID, event.EventType).Inc()
		})
		return fmt.Errorf("save event: %w", saveErr)
	}

	logger.Info("event persisted", "status", "success")
	h.inc(func(m *metrics.Metrics) {
		m.EventsProcessedTotal.WithLabelValues(event.TenantID, event.EventType, event.Producer).Inc()
	})
	h.observeAge(event)
	return nil
}

func (h *Handler) sendToDLQ(ctx context.Context, original []byte, reason string) error {
	if err := h.dlq.Publish(ctx, original, reason); err != nil {
		slog.Error("DLQ publish failed", "error_reason", err.Error())
		return fmt.Errorf("dlq publish: %w", err)
	}
	return nil
}

func (h *Handler) inc(fn func(*metrics.Metrics)) {
	if h.metrics != nil {
		fn(h.metrics)
	}
}

// observe records the processing duration labeled by event_type.
// Use "unknown" when the event type is not available (e.g. unmarshal failure).
func (h *Handler) observe(start time.Time, eventType string) {
	if h.metrics != nil {
		h.metrics.ProcessingDuration.WithLabelValues(eventType).Observe(time.Since(start).Seconds())
	}
}

// observeAge records the delay between event.OccurredAt and now.
// Called only for successfully processed events.
func (h *Handler) observeAge(event domain.Event) {
	if h.metrics != nil {
		age := time.Since(event.OccurredAt).Seconds()
		h.metrics.MessageAgeSeconds.WithLabelValues(event.EventType).Observe(age)
	}
}
