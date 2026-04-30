GO     := go
DOCKER := docker compose -f infra/docker-compose.yml

.PHONY: up down create-topic producer processor logs tidy build

## up: start Kafka in the background
up:
	$(DOCKER) up -d

## down: stop Kafka and remove volumes
down:
	$(DOCKER) down -v

## create-topic: create the raw-events topic inside the running Kafka container
create-topic:
	bash scripts/create-topics.sh

## producer: publish a sample event to raw-events
producer:
	$(GO) run ./cmd/producer

## processor: start the consumer (Ctrl+C to stop)
processor:
	$(GO) run ./cmd/processor

## logs: follow Kafka container logs
logs:
	$(DOCKER) logs -f

## build: compile both binaries into ./bin/
build:
	$(GO) build -o bin/producer ./cmd/producer
	$(GO) build -o bin/processor ./cmd/processor

## tidy: download and tidy dependencies
tidy:
	$(GO) mod tidy
