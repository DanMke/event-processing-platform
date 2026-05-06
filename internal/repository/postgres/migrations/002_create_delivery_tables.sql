-- Delivery targets: connection config per tenant
CREATE TABLE IF NOT EXISTS delivery_targets (
    id         TEXT        PRIMARY KEY,
    tenant_id  TEXT        NOT NULL,
    kind       TEXT        NOT NULL,      -- webhook | kafka | sqs
    config     JSONB       NOT NULL,
    active     BOOLEAN     NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_delivery_targets_tenant
    ON delivery_targets (tenant_id)
    WHERE active = true;

-- Outbox: one row per event/target, consumed by sender
CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL   PRIMARY KEY,
    event_id     TEXT        NOT NULL,
    tenant_id    TEXT        NOT NULL,
    target_id    TEXT        NOT NULL REFERENCES delivery_targets(id),
    payload      JSONB       NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'pending',  -- pending | sent | failed
    attempts     INT         NOT NULL DEFAULT 0,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at      TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_outbox_event_target UNIQUE (event_id, tenant_id, target_id)
);

-- Sender polls pending records ordered by scheduled_at, per target
CREATE INDEX IF NOT EXISTS idx_outbox_sender
    ON outbox (target_id, scheduled_at)
    WHERE status = 'pending';

-- Retry: failed records whose backoff window has passed
CREATE INDEX IF NOT EXISTS idx_outbox_retry
    ON outbox (scheduled_at)
    WHERE status = 'failed';

-- Demo targets: tenant-01 has two targets to exercise the 1:N outbox behaviour
INSERT INTO delivery_targets (id, tenant_id, kind, config) VALUES
    ('webhook-tenant-00', 'tenant-00', 'webhook',
     '{"url":"http://localhost:9000/events","timeout_seconds":10}'),

    ('webhook-tenant-01', 'tenant-01', 'webhook',
     '{"url":"http://localhost:9001/events","timeout_seconds":10,"headers":{"X-Api-Key":"key-tenant-01"}}'),
    ('kafka-tenant-01',   'tenant-01', 'kafka',
     '{"brokers":["kafka:29092"],"topic":"tenant-01-events"}'),

    ('webhook-tenant-02', 'tenant-02', 'webhook',
     '{"url":"http://localhost:9002/events","timeout_seconds":10,"headers":{"X-Api-Key":"key-tenant-02"}}'),

    ('kafka-tenant-03',   'tenant-03', 'kafka',
     '{"brokers":["kafka:29092"],"topic":"tenant-03-events"}'),

    ('sqs-tenant-04',     'tenant-04', 'sqs',
     '{"queue_url":"http://localhost:4566/000000000000/tenant-04-events","region":"us-east-1"}')
ON CONFLICT (id) DO NOTHING;
