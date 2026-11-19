-- Durations are stored as milliseconds, timestamps as timestamptz. Statuses
-- are plain text with a check constraint rather than an enum type, so adding a
-- status later is a one-line migration.

CREATE TABLE dally_queues (
    name                  TEXT PRIMARY KEY,
    visibility_timeout_ms BIGINT NOT NULL,
    max_attempts          INTEGER NOT NULL,
    backoff_base_ms       BIGINT NOT NULL,
    backoff_cap_ms        BIGINT NOT NULL,
    retention_ms          BIGINT NOT NULL,
    paused                BOOLEAN NOT NULL DEFAULT FALSE,
    push                  JSONB,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE dally_messages (
    id                    UUID PRIMARY KEY,
    queue                 TEXT NOT NULL REFERENCES dally_queues (name) ON DELETE CASCADE,
    payload               JSONB NOT NULL,
    status                TEXT NOT NULL
        CHECK (status IN ('scheduled', 'running', 'succeeded', 'cancelled', 'dead')),
    priority              INTEGER NOT NULL DEFAULT 0,
    attempts              INTEGER NOT NULL DEFAULT 0,
    max_attempts          INTEGER NOT NULL,
    visibility_timeout_ms BIGINT NOT NULL,
    idempotency_key       TEXT,
    run_at                TIMESTAMPTZ NOT NULL,
    lease_token           TEXT,
    lease_expires_at      TIMESTAMPTZ,
    last_error            TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at           TIMESTAMPTZ
);

-- Idempotency: one message per key per queue, for as long as it is stored.
CREATE UNIQUE INDEX dally_messages_idempotency_key
    ON dally_messages (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Polling: due messages in priority order.
CREATE INDEX dally_messages_due
    ON dally_messages (queue, priority DESC, run_at)
    WHERE status = 'scheduled';

-- Polling: running messages whose lease may have expired.
CREATE INDEX dally_messages_leases
    ON dally_messages (queue, lease_expires_at)
    WHERE status = 'running';

-- Listing a queue's messages newest first, optionally by status.
CREATE INDEX dally_messages_listing ON dally_messages (queue, status, id DESC);

-- Pruning finished messages.
CREATE INDEX dally_messages_finished
    ON dally_messages (finished_at)
    WHERE finished_at IS NOT NULL;
