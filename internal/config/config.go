package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Processor struct {
	Brokers []string
	Topic   string
	GroupID string
	Workers int
}

type Producer struct {
	Brokers []string
	Topic   string
}

type Postgres struct {
	DSN      string
	MaxConns int32
}

type DLQ struct {
	Brokers []string
	Topic   string
}

type ObservabilityConfig struct {
	MetricsPort string
}

type Sender struct {
	PollInterval time.Duration
	BatchSize    int
}

func LoadProcessor() Processor {
	return Processor{
		Brokers: splitBrokers(getEnv("KAFKA_BROKERS", "localhost:9092")),
		Topic:   getEnv("KAFKA_TOPIC", "raw-events"),
		GroupID: getEnv("KAFKA_GROUP_ID", "event-processor"),
		Workers: getEnvInt("KAFKA_WORKERS", 1),
	}
}

func LoadProducer() Producer {
	return Producer{
		Brokers: splitBrokers(getEnv("KAFKA_BROKERS", "localhost:9092")),
		Topic:   getEnv("KAFKA_TOPIC", "raw-events"),
	}
}

func LoadPostgres() Postgres {
	return Postgres{
		DSN:      getEnv("POSTGRES_DSN", "postgres://events:events@localhost:5432/events?sslmode=disable"),
		MaxConns: int32(getEnvInt("POSTGRES_MAX_CONNS", 10)),
	}
}

func LoadDLQ() DLQ {
	return DLQ{
		Brokers: splitBrokers(getEnv("KAFKA_BROKERS", "localhost:9092")),
		Topic:   getEnv("KAFKA_DLQ_TOPIC", "failed-events"),
	}
}

func LoadObservability() ObservabilityConfig {
	return ObservabilityConfig{
		MetricsPort: getEnv("METRICS_PORT", "2112"),
	}
}

func LoadSender() Sender {
	return Sender{
		PollInterval: time.Duration(getEnvInt("SENDER_POLL_INTERVAL_MS", 1000)) * time.Millisecond,
		BatchSize:    getEnvInt("SENDER_BATCH_SIZE", 50),
	}
}

func splitBrokers(v string) []string {
	return strings.Split(v, ",")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}
