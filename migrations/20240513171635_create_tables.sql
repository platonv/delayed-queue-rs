CREATE TABLE DelayedQueue (
    pkey VARCHAR(200) NOT NULL,
    pkind VARCHAR(100) NOT NULL,
    payload BYTEA NOT NULL,
    scheduled_at BIGINT NOT NULL,
    scheduled_at_initially BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (pkey, pkind)
);

CREATE INDEX DelayedQueue__KindPlusScheduledAtIndex ON DelayedQueue (pkind, scheduled_at);