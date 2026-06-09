CREATE TABLE business_record (
    record_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    source_event_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE outbox_event (
    id UUID PRIMARY KEY,
    record_id VARCHAR(100) NOT NULL,
    event_key VARCHAR(255),
    target_topic VARCHAR(255) NOT NULL,
    source_topic VARCHAR(255) NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    payload_bytes BYTEA NOT NULL,
    headers_json TEXT,
    correlation_id VARCHAR(255),
    status VARCHAR(50) NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    last_error TEXT,
    CONSTRAINT uk_outbox_source_position UNIQUE (source_topic, source_partition, source_offset)
);

CREATE INDEX idx_outbox_record_status ON outbox_event(record_id, status);
CREATE INDEX idx_outbox_publish_order ON outbox_event(status, source_topic, source_partition, source_offset);
