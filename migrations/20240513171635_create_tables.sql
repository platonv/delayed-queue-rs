CREATE TABLE delayed_queue (
    pkey VARCHAR(200) NOT NULL,
    pkind VARCHAR(100) NOT NULL,
    payload BYTEA NOT NULL,
    scheduled_at BIGINT NOT NULL,
    scheduled_at_initially BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (pkey, pkind)
);

CREATE TABLE webhooks (
    id VARCHAR(200) NOT NULL,
    url VARCHAR(200) NOT NULL,
    fails_count INT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX delayed_queue__kind_scheduled_at_index ON delayed_queue (pkind, scheduled_at);