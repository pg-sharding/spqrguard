-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION spqrguard" to load this file. \quit

CREATE SCHEMA spqr_metadata;

CREATE TABLE spqr_metadata.spqr_distributed_relations
(
    reloid OID REFERENCES pg_class(oid)
);

