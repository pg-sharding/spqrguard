-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION spqrguard" to load this file. \quit

CREATE SCHEMA spqr_metadata;

CREATE TABLE spqr_metadata.spqr_global_settings (
    name INTEGER UNIQUE,
    value TEXT
);

/*
* values
* 42 = prevent_distributed_table_modify
*/

CREATE TABLE spqr_metadata.spqr_distributed_relations (
    reloid OID PRIMARY KEY REFERENCES pg_class(oid)
);

CREATE TABLE spqr_metadata.spqr_reference_relations (
    reloid OID PRIMARY KEY REFERENCES pg_class(oid)
);

CREATE FUNCTION
spqr_metadata.mark_distributed_relation (reloid OID) 
RETURNS VOID AS
$$
INSERT INTO spqr_metadata.spqr_distributed_relations VALUES (reloid);
$$
LANGUAGE SQL;

CREATE FUNCTION
spqr_metadata.mark_distributed_relation (relname TEXT)
RETURNS VOID AS
$$
SELECT spqr_metadata.mark_distributed_relation((relname)::regclass::oid);
$$
LANGUAGE SQL;

CREATE FUNCTION
spqr_metadata.unmark_distributed_relation (reloid OID)
RETURNS VOID AS
$$
DELETE FROM spqr_metadata.spqr_distributed_relations WHERE spqr_distributed_relations.reloid = unmark_distributed_relation.reloid;
$$
LANGUAGE SQL;

CREATE FUNCTION
spqr_metadata.unmark_distributed_relation (relname TEXT)
RETURNS VOID AS
$$
SELECT spqr_metadata.unmark_distributed_relation((relname)::regclass::oid);
$$
LANGUAGE SQL;


CREATE FUNCTION
spqr_metadata.mark_reference_relation (reloid OID) 
RETURNS VOID AS
$$
INSERT INTO spqr_metadata.spqr_reference_relations VALUES (reloid);
$$
LANGUAGE SQL;

CREATE FUNCTION
spqr_metadata.mark_reference_relation (relname TEXT)
RETURNS VOID AS
$$
SELECT spqr_metadata.mark_reference_relation((relname)::regclass::oid);
$$
LANGUAGE SQL;

CREATE FUNCTION
spqr_metadata.unmark_reference_relation (reloid OID)
RETURNS VOID AS
$$
DELETE FROM spqr_metadata.spqr_reference_relations WHERE spqr_reference_relations.reloid = unmark_reference_relation.reloid;
$$
LANGUAGE SQL;

CREATE FUNCTION
spqr_metadata.unmark_reference_relation (relname TEXT)
RETURNS VOID AS
$$
SELECT spqr_metadata.unmark_reference_relation((relname)::regclass::oid);
$$
LANGUAGE SQL;

-- n_lower_bound is next (neighbor) lower bound

CREATE TABLE spqr_metadata.spqr_local_key_ranges (
    spqr_distribution TEXT NOT NULL,
    key_range_id TEXT PRIMARY KEY NOT NULL,
    lower_bound BIGINT NOT NULL,
    n_lower_bound BIGINT 
);


CREATE OR REPLACE FUNCTION spqr_metadata.split_metadata_key_range (
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