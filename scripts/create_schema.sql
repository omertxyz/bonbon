
CREATE TABLE transactions (
  slot BIGINT NOT NULL,
  block_index BIGINT NOT NULL,
  signature BYTEA NOT NULL,
  transaction BYTEA
);

CREATE TABLE partitions (
  partition_key BYTEA NOT NULL,
  program_key BYTEA NOT NULL,
  slot BIGINT NOT NULL,
  block_index BIGINT NOT NULL,
  outer_index BIGINT NOT NULL,
  inner_index BIGINT NOT NULL,
  signature BYTEA NOT NULL,
  instruction BYTEA
);
