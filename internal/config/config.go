package config

import (
	"os"
	"strings"
)

type Processor struct {
	Brokers []string
	Topic   string
	GroupID string
}

type Producer struct {
	Brokers []string
	Topic   string
}

type Postgres struct {
	DSN string
}

func LoadProcessor() Processor {
	return Processor{
		Brokers: splitBrokers(getEnv("KAFKA_BROKERS", "localhost:9092")),
		Topic:   getEnv("KAFKA_TOPIC", "raw-events"),
		GroupID: getEnv("KAFKA_GROUP_ID", "event-processor"),
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
		DSN: getEnv("POSTGRES_DSN", "postgres://events:events@localhost:5432/events?sslmode=disable"),
	}
}

type DLQ struct {
	Brokers []string
	Topic   string
}

func LoadDLQ() DLQ {
	return DLQ{
		Brokers: splitBrokers(getEnv("KAFKA_BROKERS", "localhost:9092")),
		Topic:   getEnv("KAFKA_DLQ_TOPIC", "failed-events"),
	}
}

type ObservabilityConfig struct {
	MetricsPort string
}

func LoadObservability() ObservabilityConfig {
	return ObservabilityConfig{
		MetricsPort: getEnv("METRICS_PORT", "2112"),
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
