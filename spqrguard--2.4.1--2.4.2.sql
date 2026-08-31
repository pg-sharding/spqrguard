-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION spqrguard" to load this file. \quit

/*
* SPQR metadata is not a secret: any role able to query a sharded relation
* should be able to tell whether that relation is distributed or reference.
* Grant read-only access to everyone, modification still requires an explicit
* grant from the extension owner.
*/

GRANT USAGE ON SCHEMA spqr_metadata TO PUBLIC;

GRANT SELECT ON ALL TABLES IN SCHEMA spqr_metadata TO PUBLIC;
