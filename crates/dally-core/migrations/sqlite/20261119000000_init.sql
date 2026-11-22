-- The same schema as the PostgreSQL backend, in SQLite's types. STRICT tables
-- reject values of the wrong type instead of silently storing them.
--
-- Durations are stored as milliseconds. Timestamps are milliseconds since the
-- Unix epoch, so they sort and compare as plain integers. Ids are UUIDv7 in
-- their hyphenated text form, which sorts like the UUID itself and stays
-- readable in the sqlite3 shell. JSON columns hold text.

CREATE TABLE dally_queues (
    name                  TEXT PRIMARY KEY,
    visibility_timeout_ms INTEGER NOT NULL,
    max_attempts          INTEGER NOT NULL,
    backoff_base_ms       INTEGER NOT NULL,
    backoff_cap_ms        INTEGER NOT NULL,
    retention_ms          INTEGER NOT NULL,
    paused                INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
    push                  TEXT,
    created_at            INTEGER NOT NULL,
    updated_at            INTEGER NOT NULL
) STRICT;

CREATE TABLE dally_messages (
    id                    TEXT PRIMARY KEY,
    queue                 TEXT NOT NULL REFERENCES dally_queues (name) ON DELETE CASCADE,
    payload               TEXT NOT NULL,
    status                TEXT NOT NULL
        CHECK (status IN ('scheduled', 'running', 'succeeded', 'cancelled', 'dead')),
    priority              INTEGER NOT NULL DEFAULT 0,
    attempts              INTEGER NOT NULL DEFAULT 0,
    max_attempts          INTEGER NOT NULL,
    visibility_timeout_ms INTEGER NOT NULL,
    idempotency_key       TEXT,
    run_at                INTEGER NOT NULL,
    lease_token           TEXT,
    lease_expires_at      INTEGER,
    last_error            TEXT,
    created_at            INTEGER NOT NULL,
    updated_at            INTEGER NOT NULL,
    finished_at           INTEGER
) STRICT;

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
