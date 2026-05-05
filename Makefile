GO           := go
DOCKER       := docker compose -f infra/docker-compose.yml
DOCKER_LOAD  := docker compose -f infra/docker-compose.yml --profile loadgen

TOTAL_EVENTS    ?= 1000
TENANTS         ?= 5
INVALID_RATIO   ?= 0.05
DUPLICATE_RATIO ?= 0.02
CONCURRENCY     ?= 4
SCALE           ?= 3
KAFKA_WORKERS   ?= 4
POSTGRES_MAX_CONNS ?= 20

.PHONY: up down logs create-topic migrate producer processor \
        load-test load-test-docker scale-processor \
        test fmt tidy build docker-build metrics ps \
        demo demo-scale

up:
	$(DOCKER) up -d

down:
	$(DOCKER) down -v

logs:
	$(DOCKER) logs -f

create-topic:
	bash scripts/create-topics.sh

migrate:
	docker exec -i postgres psql -U events -d events \
		< internal/repository/postgres/migrations/001_create_events_table.sql

producer:
	$(GO) run ./cmd/producer

processor:
	$(GO) run ./cmd/processor

load-test:
	TOTAL_EVENTS=$(TOTAL_EVENTS) TENANTS=$(TENANTS) \
	INVALID_RATIO=$(INVALID_RATIO) DUPLICATE_RATIO=$(DUPLICATE_RATIO) \
	CONCURRENCY=$(CONCURRENCY) \
	$(GO) run ./cmd/loadgen

load-test-docker:
	TOTAL_EVENTS=$(TOTAL_EVENTS) TENANTS=$(TENANTS) \
	INVALID_RATIO=$(INVALID_RATIO) DUPLICATE_RATIO=$(DUPLICATE_RATIO) \
	CONCURRENCY=$(CONCURRENCY) \
	$(DOCKER_LOAD) run --rm loadgen

scale-processor:
	KAFKA_WORKERS=$(KAFKA_WORKERS) POSTGRES_MAX_CONNS=$(POSTGRES_MAX_CONNS) \
	$(DOCKER) up -d --scale processor=$(SCALE)
	@echo ""
	@echo "$(SCALE) instâncias do processor rodando (workers=$(KAFKA_WORKERS), pg_max_conns=$(POSTGRES_MAX_CONNS))."
	@echo "Kafka UI → http://localhost:8080 → Consumer Groups → event-processor"

test:
	$(GO) test ./...

fmt:
	$(GO) fmt ./...

tidy:
	$(GO) mod tidy

build:
	$(GO) build -o bin/processor ./cmd/processor
	$(GO) build -o bin/producer  ./cmd/producer
	$(GO) build -o bin/loadgen   ./cmd/loadgen

docker-build:
	docker build --build-arg BINARY=processor -t event-processor:latest .
	docker build --build-arg BINARY=loadgen   -t event-loadgen:latest   .

metrics:
	@CONTAINER=$$(docker ps -q --filter "label=com.docker.compose.service=processor" | head -1); \
	if [ -n "$$CONTAINER" ]; then \
		docker exec $$CONTAINER wget -qO- http://localhost:2112/metrics; \
	else \
		curl -s http://localhost:2112/metrics; \
	fi

ps:
	$(DOCKER) ps

demo:
	$(DOCKER) up -d kafka postgres kafka-ui prometheus
	$(MAKE) create-topic
	$(MAKE) migrate
	$(DOCKER) up -d processor
	@echo "Aguardando processor inicializar..."
	sleep 3
	$(MAKE) load-test-docker TOTAL_EVENTS=500 TENANTS=5 INVALID_RATIO=0.05 DUPLICATE_RATIO=0.02 CONCURRENCY=4

demo-scale:
	$(DOCKER) up -d kafka postgres kafka-ui prometheus
	$(MAKE) create-topic
	$(MAKE) migrate
	$(MAKE) scale-processor SCALE=3
	@echo "Aguardando processors inicializarem..."
	sleep 5
	$(MAKE) load-test-docker TOTAL_EVENTS=5000 TENANTS=10 INVALID_RATIO=0.05 DUPLICATE_RATIO=0.02 CONCURRENCY=20
