CREATE TABLE IF NOT EXISTS __DHARMA_SCHEMA__.objects (
    envelope_id BYTEA PRIMARY KEY,
    bytes BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS __DHARMA_SCHEMA__.semantic_index (
    assertion_id BYTEA NOT NULL,
    envelope_id BYTEA NOT NULL,
    inserted_at BIGSERIAL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS __DHARMA_SCHEMA__.cqrs_reverse (
    envelope_id BYTEA NOT NULL,
    assertion_id BYTEA NOT NULL,
    subject_id BYTEA NOT NULL,
    is_overlay BOOLEAN NOT NULL,
    inserted_at BIGSERIAL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS __DHARMA_SCHEMA__.subject_assertions (
    subject_id BYTEA NOT NULL,
    seq BIGINT NOT NULL,
    assertion_id BYTEA NOT NULL,
    envelope_id BYTEA NOT NULL,
    bytes BYTEA NOT NULL,
    is_overlay BOOLEAN NOT NULL DEFAULT FALSE,
    inserted_at BIGSERIAL NOT NULL,
    PRIMARY KEY (subject_id, is_overlay, seq, assertion_id)
);

CREATE TABLE IF NOT EXISTS __DHARMA_SCHEMA__.permission_summaries (
    contract_id BYTEA PRIMARY KEY,
    bytes BYTEA NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_semantic_assertion
    ON __DHARMA_SCHEMA__.semantic_index(assertion_id, inserted_at DESC);
CREATE INDEX IF NOT EXISTS idx_cqrs_envelope
    ON __DHARMA_SCHEMA__.cqrs_reverse(envelope_id, inserted_at DESC);
CREATE INDEX IF NOT EXISTS idx_cqrs_assertion
    ON __DHARMA_SCHEMA__.cqrs_reverse(assertion_id, inserted_at DESC);
CREATE INDEX IF NOT EXISTS idx_subject_assertions_subject_seq
    ON __DHARMA_SCHEMA__.subject_assertions(subject_id, is_overlay, seq ASC, assertion_id ASC);
