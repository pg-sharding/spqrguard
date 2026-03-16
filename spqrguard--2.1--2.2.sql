-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION spqrguard" to load this file. \quit

CREATE TYPE tx_status AS ENUM ('planned', 'committing', 'committed', 'rejected');

CREATE TABLE spqr_metadata.spqr_tx_status (
    id TEXT PRIMARY KEY,
    members TEXT[],
    status tx_status,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);