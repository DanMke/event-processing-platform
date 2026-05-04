package config_test

import (
	"testing"

	"github.com/DanMke/event-processing-platform/internal/config"
)

func TestLoadProcessor_Defaults(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("KAFKA_TOPIC", "")
	t.Setenv("KAFKA_GROUP_ID", "")

	cfg := config.LoadProcessor()

	if len(cfg.Brokers) != 1 || cfg.Brokers[0] != "localhost:9092" {
		t.Errorf("unexpected brokers: %v", cfg.Brokers)
	}
	if cfg.Topic != "raw-events" {
		t.Errorf("unexpected topic: %s", cfg.Topic)
	}
	if cfg.GroupID != "event-processor" {
		t.Errorf("unexpected group_id: %s", cfg.GroupID)
	}
}

func TestLoadProcessor_FromEnv(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	t.Setenv("KAFKA_TOPIC", "custom-topic")
	t.Setenv("KAFKA_GROUP_ID", "custom-group")

	cfg := config.LoadProcessor()

	if len(cfg.Brokers) != 2 {
		t.Fatalf("expected 2 brokers, got %d", len(cfg.Brokers))
	}
	if cfg.Brokers[0] != "broker1:9092" || cfg.Brokers[1] != "broker2:9092" {
		t.Errorf("unexpected brokers: %v", cfg.Brokers)
	}
	if cfg.Topic != "custom-topic" {
		t.Errorf("unexpected topic: %s", cfg.Topic)
	}
	if cfg.GroupID != "custom-group" {
		t.Errorf("unexpected group_id: %s", cfg.GroupID)
	}
}

func TestLoadProducer_Defaults(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("KAFKA_TOPIC", "")

	cfg := config.LoadProducer()

	if len(cfg.Brokers) != 1 || cfg.Brokers[0] != "localhost:9092" {
		t.Errorf("unexpected brokers: %v", cfg.Brokers)
	}
	if cfg.Topic != "raw-events" {
		t.Errorf("unexpected topic: %s", cfg.Topic)
	}
}

func TestLoadPostgres_Defaults(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "")

	cfg := config.LoadPostgres()

	want := "postgres://events:events@localhost:5432/events?sslmode=disable"
	if cfg.DSN != want {
		t.Errorf("expected DSN %q, got %q", want, cfg.DSN)
	}
}

func TestLoadPostgres_FromEnv(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://user:pass@customhost:5432/mydb")

	cfg := config.LoadPostgres()

	want := "postgres://user:pass@customhost:5432/mydb"
	if cfg.DSN != want {
		t.Errorf("expected DSN %q, got %q", want, cfg.DSN)
	}
}
