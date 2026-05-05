package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

type MessageHandler func(ctx context.Context, key, value []byte) error

type reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// ConsumerOption configures a Consumer.
type ConsumerOption func(*Consumer)

// WithWorkers sets the number of messages processed concurrently.
// Defaults to 1 (sequential). Values > 1 enable concurrent processing
// while preserving at-least-once delivery and offset ordering.
func WithWorkers(n int) ConsumerOption {
	return func(c *Consumer) {
		if n > 1 {
			c.workers = n
		}
	}
}

type Consumer struct {
	reader  reader
	topic   string
	group   string
	workers int
}

func NewConsumer(brokers []string, topic, groupID string, opts ...ConsumerOption) *Consumer {
	c := &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 1,
			MaxBytes: 10e6,
		}),
		topic:   topic,
		group:   groupID,
		workers: 1,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Run consumes messages from Kafka and dispatches them to handler.
//
// Concurrency model (workers > 1):
//
//	A fetcher goroutine feeds a chan-of-chans queue (pending) in fetch order.
//	Each message is processed by a worker goroutine and reports its result
//	to a dedicated buffered channel.
//	The committer goroutine reads pending in ORDER, waiting for each message
//	to finish before deciding whether to commit its offset.
//
// This guarantees:
//   - Offsets are always committed in ascending order.
//   - A failed message stops further commits; the consumer returns an error
//     so the supervisor can restart it from the uncommitted offset.
//   - Permanent errors (invalid events) go to DLQ and return nil, so they
//     never block the pipeline.
func (c *Consumer) Run(ctx context.Context, handler MessageHandler) error {
	slog.Info("consumer loop started", "topic", c.topic, "group", c.group, "workers", c.workers)
	defer slog.Info("consumer loop stopped", "topic", c.topic, "group", c.group)

	workers := c.workers
	if workers < 1 {
		workers = 1
	}

	type msgResult struct {
		msg kafka.Message
		err error
	}

	// pending is a channel of per-message result channels, enqueued in fetch order.
	// Capacity = workers: at most `workers` messages are in-flight simultaneously.
	pending := make(chan chan msgResult, workers)
	fetchErrCh := make(chan error, 1)

	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Fetcher: fetches messages and dispatches worker goroutines.
	go func() {
		defer close(pending)
		for {
			msg, err := c.reader.FetchMessage(innerCtx)
			if err != nil {
				// Only report unexpected errors; context cancellations are normal shutdown.
				if ctx.Err() == nil && innerCtx.Err() == nil {
					fetchErrCh <- fmt.Errorf("fetch message: %w", err)
				}
				return
			}

			ch := make(chan msgResult, 1)
			select {
			case pending <- ch: // reserve an ordered slot before spawning worker
				go func(m kafka.Message) {
					handlerErr := handler(ctx, m.Key, m.Value)
					ch <- msgResult{msg: m, err: handlerErr}
				}(msg)
			case <-innerCtx.Done():
				return
			}
		}
	}()

	// Committer: processes results in fetch order.
	for ch := range pending {
		r := <-ch // wait for this specific message to finish
		if r.err != nil {
			slog.Warn("handler error — stopping consumer for safe reprocessing",
				"topic", r.msg.Topic,
				"partition", r.msg.Partition,
				"offset", r.msg.Offset,
				"error_reason", r.err.Error(),
			)
			cancel() // stop fetcher
			for drain := range pending {
				<-drain // let in-flight workers finish (results discarded, no commit)
			}
			return r.err
		}

		if err := c.reader.CommitMessages(ctx, r.msg); err != nil {
			slog.Error("offset commit failed",
				"topic", r.msg.Topic,
				"partition", r.msg.Partition,
				"offset", r.msg.Offset,
				"error_reason", err.Error(),
			)
			return fmt.Errorf("commit offset %d: %w", r.msg.Offset, err)
		}
	}

	select {
	case err := <-fetchErrCh:
		return err
	default:
		return nil
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
