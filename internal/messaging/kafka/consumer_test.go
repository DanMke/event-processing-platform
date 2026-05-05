package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

// --- mock ---

type mockReader struct {
	messages  []kafka.Message
	idx       int
	commits   []kafka.Message
	fetchErr  error
	commitErr error
}

func (m *mockReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if m.fetchErr != nil {
		return kafka.Message{}, m.fetchErr
	}
	if m.idx < len(m.messages) {
		msg := m.messages[m.idx]
		m.idx++
		return msg, nil
	}
	// no more staged messages: block until the test cancels the context
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (m *mockReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	if m.commitErr != nil {
		return m.commitErr
	}
	m.commits = append(m.commits, msgs...)
	return nil
}

func (m *mockReader) Close() error { return nil }

// --- helpers ---

func newConsumerWithMock(r reader) *Consumer {
	return &Consumer{reader: r, topic: "test-topic", group: "test-group"}
}

func alwaysSucceedHandler(_ context.Context, _, _ []byte) error { return nil }

func alwaysFailHandler(_ context.Context, _, _ []byte) error {
	return errors.New("handler failure")
}

// --- tests ---

// TestRun_Success_CommitsOffset verifies that a successful handler causes the
// offset to be committed exactly once per message.
func TestRun_Success_CommitsOffset(t *testing.T) {
	r := &mockReader{
		messages: []kafka.Message{
			{Topic: "test-topic", Partition: 0, Offset: 42, Value: []byte("payload")},
		},
	}
	c := newConsumerWithMock(r)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, alwaysSucceedHandler) }()

	// wait until the message is processed and committed, then stop the loop
	for len(r.commits) == 0 {
	}
	cancel()
	<-done

	if len(r.commits) != 1 {
		t.Fatalf("expected 1 commit, got %d", len(r.commits))
	}
	if r.commits[0].Offset != 42 {
		t.Errorf("expected committed offset 42, got %d", r.commits[0].Offset)
	}
}

// TestRun_HandlerError_NoCommit verifies that when the handler returns an error
// the offset is NOT committed, preserving at-least-once delivery.
func TestRun_HandlerError_NoCommit(t *testing.T) {
	r := &mockReader{
		messages: []kafka.Message{
			{Topic: "test-topic", Partition: 0, Offset: 7, Value: []byte("payload")},
		},
	}
	c := newConsumerWithMock(r)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, alwaysFailHandler) }()

	// after the one message is consumed (and skipped), FetchMessage blocks
	// cancel the context to stop the loop
	for r.idx == 0 {
	}
	cancel()
	<-done

	if len(r.commits) != 0 {
		t.Errorf("expected 0 commits when handler errors, got %d", len(r.commits))
	}
}

// TestRun_FetchError_ReturnsError verifies that a fetch failure (other than
// context cancellation) is propagated as a non-nil error from Run.
func TestRun_FetchError_ReturnsError(t *testing.T) {
	r := &mockReader{fetchErr: errors.New("broker unavailable")}
	c := newConsumerWithMock(r)

	err := c.Run(context.Background(), alwaysSucceedHandler)

	if err == nil {
		t.Error("expected error from Run when FetchMessage fails, got nil")
	}
}

// TestRun_ContextCancelled_ReturnsNil verifies that graceful shutdown via
// context cancellation causes Run to return nil (not an error).
func TestRun_ContextCancelled_ReturnsNil(t *testing.T) {
	r := &mockReader{} // no messages: FetchMessage will block on ctx immediately
	c := newConsumerWithMock(r)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before Run even starts

	err := c.Run(ctx, alwaysSucceedHandler)

	if err != nil {
		t.Errorf("expected nil on context cancellation, got: %v", err)
	}
}

// TestRun_CommitError_ReturnsError verifies that a CommitMessages failure is
// propagated as a non-nil error from Run, preventing silent offset advancement.
func TestRun_CommitError_ReturnsError(t *testing.T) {
	r := &mockReader{
		messages: []kafka.Message{
			{Topic: "test-topic", Partition: 0, Offset: 5, Value: []byte("payload")},
		},
		commitErr: errors.New("broker unreachable"),
	}
	c := newConsumerWithMock(r)

	err := c.Run(context.Background(), alwaysSucceedHandler)

	if err == nil {
		t.Error("expected error when CommitMessages fails, got nil")
	}
}

// TestRun_MultipleMessages_AllCommitted verifies that every successfully
// processed message in a batch gets its offset committed.
func TestRun_MultipleMessages_AllCommitted(t *testing.T) {
	r := &mockReader{
		messages: []kafka.Message{
			{Offset: 10, Value: []byte("a")},
			{Offset: 11, Value: []byte("b")},
			{Offset: 12, Value: []byte("c")},
		},
	}
	c := newConsumerWithMock(r)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, alwaysSucceedHandler) }()

	for len(r.commits) < 3 {
	}
	cancel()
	<-done

	if len(r.commits) != 3 {
		t.Fatalf("expected 3 commits, got %d", len(r.commits))
	}
}
