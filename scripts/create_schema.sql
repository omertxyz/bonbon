
CREATE TABLE transactions (
    slot BIGINT NOT NULL,
    block_index BIGINT NOT NULL,
    signature BYTEA NOT NULL,
    transaction BYTEA
);
