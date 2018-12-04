CREATE TABLE domain_event (
    global_offset   BIGSERIAL PRIMARY KEY,
    event_id        VARCHAR(36)  NOT NULL,
    aggregate_id    VARCHAR(36)  NOT NULL,
    aggregate_type  VARCHAR(255) NOT NULL,
    tag             VARCHAR(100) NOT NULL,
    causation_id    VARCHAR(36)  NOT NULL,
    correlation_id  VARCHAR(36)  NULL,
    event_type      VARCHAR(255) NOT NULL,
    event_version   INT          NOT NULL,
    event_payload   TEXT         NOT NULL,
    event_timestamp TIMESTAMP    NOT NULL,
    sequence_number BIGINT       NOT NULL
);

CREATE INDEX events_for_aggregate_instance_idx ON domain_event (aggregate_type, aggregate_id);

CREATE TABLE aggregate_root (
    aggregate_id      VARCHAR(36)  NOT NULL,
    aggregate_type    VARCHAR(255) NOT NULL,
    aggregate_version BIGINT       NOT NULL,
    PRIMARY KEY (aggregate_id, aggregate_type)
);

-- We don't care if we get duplicate event ids as the process manager processing logic will filter them out
-- Assuming that enough historical event ids are retained in case of snapshotting
-- Should never be a ridiculous number of event ids in a snapshot, otherwise transaction boundary almost certainly wrong
CREATE TABLE process_manager_domain_event (
    global_offset                  BIGSERIAL    PRIMARY KEY,
    event_id                       VARCHAR(72)  NOT NULL,
    process_manager_correlation_id VARCHAR(72)  NOT NULL,
    process_manager_type           VARCHAR(255) NOT NULL,
    tag                            VARCHAR(100) NOT NULL,
    event_type                     VARCHAR(255) NOT NULL,
    event_version                  INT          NOT NULL,
    event_payload                  TEXT         NOT NULL,
    event_timestamp                TIMESTAMP    NOT NULL,
    sequence_number                BIGINT       NOT NULL
)

CREATE INDEX events_for_process_manager_instance_idx ON process_manager_domain_event (process_manager_type, process_manager_id);

CREATE TABLE process_manager (
    process_manager_correlation_id  VARCHAR(72)              NOT NULL,
    process_manager_type            VARCHAR(255)             NOT NULL,
    max_processed_sequence_number   BIGINT                   NOT NULL,
    last_processed_sequence_number  BIGINT                   NOT NULL,
    oldest_unprocessed_timestamp    TIMESTAMP WITH TIME ZONE NULL,
    has_unprocessed_events          BOOLEAN                  NOT NULL,
    retry_count                     INT                      NULL,
    retry_after                     TIMESTAMP WITH TIME ZONE NULL,
    suspended                       BOOLEAN                  NOT NULL,
    PRIMARY KEY (process_manager_id, process_manager_type)
)

CREATE TABLE process_manager_failure (
    failure_id                     BIGSERIAL                PRIMARY KEY,
    process_manager_correlation_id VARCHAR(72)              NOT NULL,
    sequence_number                BIGINT                   NOT NULL,
    failure_code                   VARCHAR(72)              NOT NULL,
    stack_trace                    TEXT                     NULL,
    message                        TEXT                     NULL,
    failure_timestamp              TIMESTAMP WITH TIME ZONE NOT NULL
)

CREATE TABLE process_manager_snapshot (

)

CREATE TABLE usr (
    id       VARCHAR(36)  PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    password VARCHAR(20)  NOT NULL,
    locked   BOOLEAN      NOT NULL
)