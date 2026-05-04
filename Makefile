GO     := go
DOCKER := docker compose -f infra/docker-compose.yml

.PHONY: up down create-topic migrate producer processor logs build tidy

## up: start Kafka, Kafka UI and Postgres in the background
up:
	$(DOCKER) up -d

## down: stop all containers and remove volumes
down:
	$(DOCKER) down -v

## create-topic: create the raw-events topic inside the running Kafka container
create-topic:
	bash scripts/create-topics.sh

## migrate: apply SQL migrations against the running Postgres container
migrate:
	docker exec -i postgres psql -U events -d events \
		< internal/repository/postgres/migrations/001_create_events_table.sql

## producer: publish sample events to raw-events
producer:
	$(GO) run ./cmd/producer

## processor: start the consumer/processor (Ctrl+C to stop)
processor:
	$(GO) run ./cmd/processor

## logs: follow all container logs
logs:
	$(DOCKER) logs -f

## build: compile both binaries into ./bin/
build:
	$(GO) build -o bin/producer ./cmd/producer
	$(GO) build -o bin/processor ./cmd/processor

## tidy: download and tidy dependencies
tidy:
	$(GO) mod tidy
