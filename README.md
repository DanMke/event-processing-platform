# event-processing-platform

Plataforma de processamento de eventos usando Go, Kafka e Postgres.

```
Producer → Kafka (raw-events) → Processor → Postgres (events)
```

## Pré-requisitos

- [Go 1.22+](https://go.dev/dl/)
- [Docker](https://docs.docker.com/get-docker/) com Docker Compose v2

## Como rodar

### 1. Subir a infraestrutura

```bash
make up
```

Sobe Kafka, Kafka UI e Postgres. Aguarda os containers ficarem saudáveis (≈20 s na primeira vez).

### 2. Criar o tópico e aplicar a migration

```bash
make create-topic
make migrate
```

### 3. Iniciar o processor

Em um terminal separado:

```bash
make processor
```

### 4. Publicar eventos

Em outro terminal:

```bash
make producer
```

O producer publica dois eventos (`contract.created` e `contract.cancelled`) e encerra.  
O processor consome, loga os metadados e persiste no Postgres.

### 5. Consultar os eventos persistidos

```bash
docker exec -it postgres psql -U events -d events -c "SELECT event_id, tenant_id, event_type, occurred_at FROM events;"
```

### Resumo: make up → make create-topic → make migrate → make processor → make producer

---

## Variáveis de ambiente

| Variável         | Padrão                                                      | Descrição                        |
|------------------|-------------------------------------------------------------|----------------------------------|
| `KAFKA_BROKERS`  | `localhost:9092`                                            | Endereço do broker Kafka         |
| `KAFKA_TOPIC`    | `raw-events`                                                | Tópico de eventos                |
| `KAFKA_GROUP_ID` | `event-processor`                                           | Consumer group (processor only)  |
| `POSTGRES_DSN`   | `postgres://events:events@localhost:5432/events?sslmode=disable` | Connection string do Postgres |

---

## Estrutura do projeto

```
event-processing-platform/
├── cmd/
│   ├── producer/           # Publica eventos no Kafka
│   └── processor/          # Consome do Kafka e persiste no Postgres
├── internal/
│   ├── config/             # Leitura de variáveis de ambiente
│   ├── domain/             # Struct Event (envelope padrão)
│   ├── messaging/kafka/    # Abstrações de producer e consumer
│   ├── processor/          # Handler: unmarshal → log → persist
│   ├── producer/           # Service: build → publish
│   └── repository/postgres/
│       ├── migrations/     # SQL migrations
│       └── repository.go   # EventRepository
├── infra/
│   └── docker-compose.yml  # Kafka, Kafka UI, Postgres
├── scripts/
│   └── create-topics.sh
├── Makefile
└── go.mod
```

## Formato do evento

```json
{
  "event_id": "01HYZK8KJ3F9Z6K2X8YQ1W0ABC",
  "tenant_id": "client-a",
  "event_type": "contract.created",
  "schema_version": "1.0",
  "occurred_at": "2026-04-28T20:00:00Z",
  "producer": "sample-producer",
  "trace_id": "trace-123",
  "payload": {
    "contract_id": "contract-123",
    "amount": 1000,
    "currency": "BRL"
  }
}
```

## Próximos passos planejados

- Idempotência por `(tenant_id, event_id)`
- Validação por JSON Schema
- Observabilidade com OpenTelemetry
- Dead-letter queue (DLQ) e retry
- Infraestrutura como código (Terraform / LocalStack)
- Teste de Carga
- Detalhar documentação