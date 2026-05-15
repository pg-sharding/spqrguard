-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION spqrguard" to load this file. \quit

CREATE FUNCTION
spqr_metadata.lock_key_range_read(v_key_range_id string, v_key_range_version int)
RETURNS BOOL
LANGUAGE C 
AS 'MODULE_PATHNAME', $$spqrguard_lock_key_range_read$$
$$
SELECT (SELECT count(*) FROM spqr_metadata.spqr_local_key_ranges WHERE key_range_id=v_key_range_id) > 0;
$$
LANGUAGE SQL;