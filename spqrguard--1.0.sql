-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION spqrguard" to load this file. \quit

CREATE SCHEMA spqr_metadata;

CREATE TABLE spqr_metadata.spqr_distributed_relations (
    reloid OID REFERENCES pg_class(oid)
);

-- n_lower_bound is next (neighbor) lower bound

CREATE TABLE spqr_metadata.spqr_local_key_ranges (
    spqr_distribution TEXT,
    key_range_id TEXT PRIMARY KEY,
    lower_bound BIGINT,
    n_lower_bound BIGINT 
);


CREATE OR REPLACE FUNCTION spqr_metadata.split_metadata_key_range(
    v_key_range_id TEXT,
    v_outer_key_range_id TEXT,
    v_split_bound BIGINT
) RETURNS VOID AS $$
BEGIN
    WITH old_row as (
        SELECT 
            spqr_distribution, n_lower_bound
        FROM
            spqr_metadata.spqr_local_key_ranges t
        WHERE t.key_range_id = v_key_range_id
    ), upd_old as (
        UPDATE 
            spqr_metadata.spqr_local_key_ranges t
        SET
            n_lower_bound = v_split_bound
        WHERE t.key_range_id = v_key_range_id
    )

    INSERT INTO 
        spqr_metadata.spqr_local_key_ranges
    SELECT
        old_row.spqr_distribution,
        v_outer_key_range_id,
        v_split_bound,
        old_row.n_lower_bound
    FROM old_row;

END;
$$ LANGUAGE PLPGSQL;