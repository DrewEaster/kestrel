CREATE TABLE domain_event (
  global_offset   BIGSERIAL PRIMARY KEY,
  event_id        VARCHAR(72)  NOT NULL,
  aggregate_id    VARCHAR(72)  NOT NULL,
  aggregate_type  VARCHAR(255) NOT NULL,
  tag             VARCHAR(100) NOT NULL,
  causation_id    VARCHAR(72)  NOT NULL,
  correlation_id  VARCHAR(72)  NULL,
  event_type      VARCHAR(255) NOT NULL,
  event_version   INT          NOT NULL,
  event_payload   TEXT         NOT NULL,
  event_timestamp TIMESTAMP    NOT NULL,
  sequence_number BIGINT       NOT NULL
);

CREATE INDEX events_for_aggregate_instance_idx ON domain_event (aggregate_type, aggregate_id);

CREATE TABLE aggregate_root (
  aggregate_id              VARCHAR(72)    NOT NULL,
  aggregate_type            VARCHAR(255)   NOT NULL,
  aggregate_version         BIGINT         NOT NULL,
  snapshot_type             VARCHAR(255)   NULL,
  snapshot_version          BIGINT         NULL,
  snapshot_payload          TEXT           NULL,
  snapshot_causation_ids    VARCHAR(72)[]  NOT NULL DEFAULT '{}',
  PRIMARY KEY (aggregate_id, aggregate_type)
);