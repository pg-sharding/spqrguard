-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION spqrguard" to load this file. \quit

/*
* SPQR metadata is not a secret: any role able to query a sharded relation
* should be able to tell whether that relation is distributed or reference.
* Grant read-only access to everyone, modification still requires an explicit
* grant from the extension owner.
*/

GRANT USAGE ON SCHEMA spqr_metadata TO PUBLIC;

GRANT SELECT ON spqr_metadata.spqr_global_settings TO PUBLIC;
GRANT SELECT ON spqr_metadata.spqr_distributed_relations TO PUBLIC;
GRANT SELECT ON spqr_metadata.spqr_reference_relations TO PUBLIC;
GRANT SELECT ON spqr_metadata.spqr_transferred_reference_relations TO PUBLIC;
GRANT SELECT ON spqr_metadata.spqr_local_key_ranges TO PUBLIC;
GRANT SELECT ON spqr_metadata.spqr_tx_status TO PUBLIC;
