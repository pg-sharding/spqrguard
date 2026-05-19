-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION spqrguard" to load this file. \quit

CREATE FUNCTION
spqr_metadata.share_key_range(v_key_range_id text)
RETURNS BOOL
LANGUAGE C 
AS 'MODULE_PATHNAME', $$spqrguard_share_key_range$$;
