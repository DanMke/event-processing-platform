package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

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
	// Block until the test cancels the context.
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

func newConsumerWithMock(r reader) *Consumer {
	return &Consumer{reader: r, topic: "test-topic", group: "test-group"}
}

func alwaysSucceedHandler(_ context.Context, _, _ []byte) error { return nil }

func alwaysFailHandler(_ context.Context, _, _ []byte) error {
	return errors.New("handler failure")
}

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

	// Stop after the first commit.
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

	// Stop after the failed message is fetched.
	for r.idx == 0 {
	}
	cancel()
	<-done

	if len(r.commits) != 0 {
		t.Errorf("expected 0 commits when handler errors, got %d", len(r.commits))
	}
}

func TestRun_FetchError_ReturnsError(t *testing.T) {
	r := &mockReader{fetchErr: errors.New("broker unavailable")}
	c := newConsumerWithMock(r)

	err := c.Run(context.Background(), alwaysSucceedHandler)

	if err == nil {
		t.Error("expected error from Run when FetchMessage fails, got nil")
	}
}

func TestRun_ContextCancelled_ReturnsNil(t *testing.T) {
	r := &mockReader{} // FetchMessage blocks until ctx is canceled.
	c := newConsumerWithMock(r)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := c.Run(ctx, alwaysSucceedHandler)

	if err != nil {
		t.Errorf("expected nil on context cancellation, got: %v", err)
	}
}

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
