GO           := go
DOCKER       := docker compose -f infra/docker-compose.yml
DOCKER_LOAD  := docker compose -f infra/docker-compose.yml --profile loadgen

TOTAL_EVENTS       ?= 1000
TENANTS            ?= 5
INVALID_RATIO      ?= 0.05
DUPLICATE_RATIO    ?= 0.02
CONCURRENCY        ?= 4
SCALE              ?= 3
KAFKA_WORKERS      ?= 4
POSTGRES_MAX_CONNS ?= 20

.PHONY: up down logs ps \
        create-topic migrate \
        build docker-build \
        test test-integration fmt tidy \
        processor producer load-test load-test-docker \
        scale-processor health metrics \
        demo demo-scale

up:
	$(DOCKER) up -d --build

down:
	$(DOCKER) down -v

logs:
	$(DOCKER) logs -f

ps:
	$(DOCKER) ps

create-topic:
	docker exec kafka sh -c "until /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list >/dev/null 2>&1; do sleep 2; done"
	docker exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:29092 \
		--create \
		--if-not-exists \
		--topic raw-events \
		--partitions 3 \
		--replication-factor 1
	docker exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:29092 \
		--create \
		--if-not-exists \
		--topic failed-events \
		--partitions 3 \
		--replication-factor 1

migrate:
	docker exec -i postgres psql -U events -d events \
		< internal/repository/postgres/migrations/001_create_events_table.sql
	docker exec -i postgres psql -U events -d events \
		< internal/repository/postgres/migrations/002_create_delivery_tables.sql

tidy:
	$(GO) mod tidy

fmt:
	$(GO) fmt ./...

build: tidy
	$(GO) build -o bin/processor ./cmd/processor
	$(GO) build -o bin/producer  ./cmd/producer
	$(GO) build -o bin/loadgen   ./cmd/loadgen

docker-build: tidy
	$(DOCKER) build processor loadgen

test:
	$(GO) test ./...

test-integration:
	$(GO) test -tags=integration -v ./internal/repository/postgres/...

processor:
	$(GO) run ./cmd/processor

producer:
	$(GO) run ./cmd/producer

load-test:
	TOTAL_EVENTS=$(TOTAL_EVENTS) TENANTS=$(TENANTS) \
	INVALID_RATIO=$(INVALID_RATIO) DUPLICATE_RATIO=$(DUPLICATE_RATIO) \
	CONCURRENCY=$(CONCURRENCY) \
	$(GO) run ./cmd/loadgen

load-test-docker:
	$(DOCKER_LOAD) run --rm --build \
		-e TOTAL_EVENTS=$(TOTAL_EVENTS) \
		-e TENANTS=$(TENANTS) \
		-e INVALID_RATIO=$(INVALID_RATIO) \
		-e DUPLICATE_RATIO=$(DUPLICATE_RATIO) \
		-e CONCURRENCY=$(CONCURRENCY) \
		loadgen

scale-processor:
	$(DOCKER) up -d --build --scale processor=$(SCALE)
	@echo "$(SCALE) processor instances running (workers=$(KAFKA_WORKERS), pg_max_conns=$(POSTGRES_MAX_CONNS))"
	@echo "Kafka UI -> http://localhost:8080 -> Consumer Groups -> event-processor"

health:
	$(DOCKER) exec -T processor wget -qO- http://localhost:2112/healthz

metrics:
	$(DOCKER) exec -T processor wget -qO- http://localhost:2112/metrics

demo: docker-build
	$(DOCKER) up -d kafka postgres kafka-ui
	$(MAKE) create-topic
	$(MAKE) migrate
	$(DOCKER) up -d --build processor
	$(DOCKER) up -d prometheus
	$(MAKE) load-test-docker TOTAL_EVENTS=500 TENANTS=5 INVALID_RATIO=0.05 DUPLICATE_RATIO=0.02 CONCURRENCY=4

demo-scale: docker-build
	$(DOCKER) up -d kafka postgres kafka-ui
	$(MAKE) create-topic
	$(MAKE) migrate
	$(MAKE) scale-processor SCALE=3
	$(DOCKER) up -d prometheus
	$(MAKE) load-test-docker TOTAL_EVENTS=5000 TENANTS=10 INVALID_RATIO=0.05 DUPLICATE_RATIO=0.02 CONCURRENCY=20
