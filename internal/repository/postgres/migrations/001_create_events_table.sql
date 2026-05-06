CREATE TABLE IF NOT EXISTS events (
    event_id       TEXT        NOT NULL,
    tenant_id      TEXT        NOT NULL,
    event_type     TEXT        NOT NULL,
    schema_version TEXT        NOT NULL,
    producer       TEXT        NOT NULL,
    trace_id       TEXT,
    payload        JSONB       NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL,
    processed_at   TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (tenant_id, event_id)
);
