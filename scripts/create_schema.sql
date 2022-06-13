
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
  inner_index BIGINT,
  signature BYTEA NOT NULL,
  instruction BYTEA
);

CREATE TYPE token_meta AS (
  account_index SMALLINT,
  mint_key BYTEA,
  owner_key BYTEA
);

CREATE TABLE account_keys (
  signature BYTEA PRIMARY KEY,
  keys BYTEA[],
  metas token_meta[]
);


CREATE TYPE edition_status AS enum (
  'none',
  'master',
  'limited'
);

CREATE TYPE limited_edition AS (
  master_key BYTEA,
  -- u64 but close enough...
  edition_num BIGINT
);

CREATE TABLE bonbons (
  metadata_key BYTEA NOT NULL,
  mint_key BYTEA NOT NULL,
  current_owner BYTEA,
  current_account BYTEA,
  edition_status edition_status NOT NULL,
  limited_edition limited_edition,
  uri BYTEA
);

CREATE TABLE creators (
  creator_key BYTEA NOT NULL,
  metadata_key BYTEA NOT NULL,
  verified BOOLEAN,
  share SMALLINT,

  UNIQUE (creator_key, metadata_key)
);

CREATE TABLE collections (
  collection_key BYTEA NOT NULL,
  metadata_key BYTEA NOT NULL,
  verified BOOLEAN,

  UNIQUE (collection_key, metadata_key)
);
