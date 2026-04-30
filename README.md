# event-processing-platform

MVP local de um fluxo de eventos usando Go, Kafka e Docker.

```
Producer → Kafka (raw-events) → Processor (logs)
```

## Pré-requisitos

- [Go 1.22+](https://go.dev/dl/)
- [Docker](https://docs.docker.com/get-docker/) com Docker Compose v2

## Como rodar

### 1. Subir o Kafka

```bash
make up
```

Aguarda o container `kafka` ficar saudável (≈15 s na primeira vez).

### 2. Criar o tópico `raw-events`

```bash
make create-topic
```

### 3. Iniciar o processor (consumer)

Em um terminal separado:

```bash
make processor
```

### 4. Publicar um evento

Em outro terminal:

```bash
make producer
```

O producer imprime o evento publicado e encerra.  
O processor exibe a linha de log correspondente.

---

## Variáveis de ambiente

| Variável         | Padrão          | Descrição                        |
|------------------|-----------------|----------------------------------|
| `KAFKA_BROKERS`  | `localhost:9092` | Endereço do broker Kafka         |
| `KAFKA_TOPIC`    | `raw-events`    | Tópico de eventos                |
| `KAFKA_GROUP_ID` | `event-processor` | Consumer group (processor only) |

---

## Estrutura do projeto

```
event-processing-platform/
├── cmd/
│   ├── producer/       # Publica eventos no Kafka
│   └── processor/      # Consome e loga eventos do Kafka
├── internal/
│   ├── domain/         # Struct Event
│   └── messaging/kafka # Abstrações de producer e consumer
├── infra/
│   └── docker-compose.yml
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

- Persistência (PostgreSQL)
- Idempotência por `event_id`
- Validação por JSON Schema
- Observabilidade com OpenTelemetry
- Dead-letter queue (DLQ) e retry
- Infraestrutura como código (Terraform)
