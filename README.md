# event-processing-platform

Plataforma de processamento de eventos usando Go, Kafka e Postgres.

```
Producer → Kafka (raw-events) → Processor → Postgres (events)
                                     ↓
                              Kafka (failed-events)  ← eventos inválidos
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

### 2. Criar os tópicos e aplicar a migration

```bash
make create-topic
make migrate
```

Cria os tópicos `raw-events` e `failed-events`.

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
O processor consome, valida, persiste no Postgres e encaminha eventos inválidos para a DLQ.

### 5. Consultar os eventos persistidos

```bash
docker exec -it postgres psql -U events -d events -c \
  "SELECT event_id, tenant_id, event_type, occurred_at FROM events;"
```

### 6. Visualizar tópicos no Kafka UI

Acesse [http://localhost:8080](http://localhost:8080) e selecione o cluster `local`.

- **raw-events** → eventos publicados pelo producer
- **failed-events** → eventos rejeitados (envelope inválido, schema inválido, JSON malformado)

### 7. Consultar métricas do processor

```bash
curl http://localhost:2112/metrics
```

### Resumo: make up && make create-topic && make migrate → make processor → make producer

---

## Variáveis de ambiente

| Variável          | Padrão                                                           | Descrição                        |
|-------------------|------------------------------------------------------------------|----------------------------------|
| `KAFKA_BROKERS`   | `localhost:9092`                                                 | Endereço do broker Kafka         |
| `KAFKA_TOPIC`     | `raw-events`                                                     | Tópico de eventos                |
| `KAFKA_GROUP_ID`  | `event-processor`                                                | Consumer group (processor only)  |
| `KAFKA_DLQ_TOPIC` | `failed-events`                                                  | Tópico de dead letter            |
| `POSTGRES_DSN`    | `postgres://events:events@localhost:5432/events?sslmode=disable` | Connection string do Postgres    |
| `METRICS_PORT`    | `2112`                                                           | Porta do servidor de métricas    |
| `LOG_FORMAT`      | `text`                                                           | Formato dos logs (`text` ou `json`) |

---

## Estrutura do projeto

```
event-processing-platform/
├── cmd/
│   ├── producer/              # Publica eventos no Kafka
│   └── processor/             # Consome do Kafka, valida e persiste
├── internal/
│   ├── config/                # Leitura de variáveis de ambiente
│   ├── dlq/                   # Dead-letter queue publisher
│   ├── domain/                # Struct Event e erros de domínio
│   ├── messaging/kafka/       # Abstrações de producer e consumer
│   ├── observability/
│   │   ├── logger.go          # Inicialização do slog (text/json via LOG_FORMAT)
│   │   └── metrics/           # Métricas Prometheus do processor
│   ├── processor/             # Handler: unmarshal → validate → retry → persist
│   ├── producer/              # Service: build → publish
│   ├── repository/postgres/
│   │   ├── migrations/        # SQL migrations
│   │   └── repository.go      # EventRepository
│   ├── retry/                 # Retry com backoff incremental
│   └── validation/
│       ├── schemas/           # JSON Schemas por event_type e schema_version
│       ├── envelope.go        # Validação dos campos obrigatórios do envelope
│       └── schema.go          # Validação do payload por JSON Schema
├── infra/
│   └── docker-compose.yml     # Kafka, Kafka UI, Postgres
├── scripts/
│   └── create-topics.sh       # Cria raw-events e failed-events
├── Makefile
└── go.mod
```

---

## Pipeline do processor

```
Kafka (raw-events)
  │
  ▼
unmarshal JSON
  ├─ erro → DLQ (failed-events) + offset confirmado
  ▼
validar envelope (campos obrigatórios)
  ├─ inválido → DLQ (failed-events) + offset confirmado
  ▼
validar payload por JSON Schema
  ├─ inválido ou schema desconhecido → DLQ (failed-events) + offset confirmado
  ▼
persistir no Postgres (com retry: 100ms → 300ms → 500ms)
  ├─ duplicata → ignorado como sucesso (idempotência)
  ├─ erro transitório esgotado → offset NÃO confirmado
  ▼
evento persistido
```

---

## Comportamentos-chave

### Idempotência
A tabela `events` usa `PRIMARY KEY (tenant_id, event_id)`. O repository detecta `ON CONFLICT DO NOTHING` via `RowsAffected() == 0` e retorna `domain.ErrDuplicateEvent`. O handler trata como sucesso controlado -> o evento não é salvo novamente nem enviado à DLQ.

### Validação de envelope
Campos obrigatórios verificados antes de qualquer persistência: `event_id`, `tenant_id`, `event_type`, `schema_version`, `producer`, `occurred_at`, `payload`. Evento com campos ausentes vai para a DLQ.

### Validação de payload por schema
O schema é selecionado por `event_type + schema_version` (ex: `contract.created/1.0`). Os schemas ficam em `internal/validation/schemas/` e são carregados via `embed.FS` -> nenhum I/O em runtime. Payload inválido ou tipo desconhecido vai para a DLQ.

### Dead-letter queue (DLQ)
Erros permanentes (unmarshal, envelope, schema) publicam no tópico `failed-events` com o evento original + motivo + timestamp. O offset é confirmado. Erros transitórios (banco de dados) não vão para a DLQ -> o offset fica pendente para reprocessamento.

### Retry
Falhas de persistência disparam até 3 retries com backoff incremental (100ms, 300ms, 500ms). Se o banco permanecer indisponível, o offset não é confirmado, garantindo que o evento seja reprocessado quando o banco voltar.

---

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

## Formato da mensagem na DLQ

```json
{
  "original_event": { "...evento original..." },
  "error_reason": "invalid envelope: missing=tenant_id,occurred_at",
  "failed_at": "2026-05-03T20:00:00Z",
  "source_topic": "raw-events"
}
```

---

## Métricas

O processor expõe um endpoint Prometheus em `http://localhost:2112/metrics` (porta configurável via `METRICS_PORT`).

| Métrica | Tipo | Descrição |
|---|---|---|
| `events_processed_total` | Counter | Eventos validados e persistidos com sucesso |
| `events_failed_total` | Counter | Eventos que falharam por erro transitório após retries |
| `events_invalid_total` | Counter | Eventos rejeitados por JSON, envelope ou payload inválido |
| `events_duplicated_total` | Counter | Duplicatas ignoradas (idempotência) |
| `events_sent_to_dlq_total` | Counter | Eventos encaminhados para a DLQ |
| `event_processing_duration_seconds` | Histogram | Tempo de processamento por evento (unmarshal → persist) |

Além dessas, o endpoint também expõe métricas padrão do runtime Go (`go_*`) e do processo (`process_*`).

**Exemplo de saída:**
```
events_processed_total 2
events_invalid_total 0
events_duplicated_total 0
events_failed_total 0
events_sent_to_dlq_total 0
event_processing_duration_seconds_bucket{le="0.005"} 2
```

---

## Logs estruturados

Os logs usam `log/slog` com campos consistentes em todos os eventos:

```
time=2026-05-04T20:00:00Z level=INFO msg="event received" event_id=evt-001 tenant_id=client-a event_type=contract.created schema_version=1.0 producer=sample-producer trace_id=trace-123
time=2026-05-04T20:00:00Z level=INFO msg="event persisted" event_id=evt-001 tenant_id=client-a event_type=contract.created schema_version=1.0 producer=sample-producer trace_id=trace-123 status=success
```

Para formato JSON (recomendado em produção):
```bash
LOG_FORMAT=json make processor
```

---

## Próximos passos planejados

- OpenTelemetry (tracing distribuído)
- Infraestrutura como código (Terraform / LocalStack)
- Teste de carga
