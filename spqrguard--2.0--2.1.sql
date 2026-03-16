-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION spqrguard" to load this file. \quit

DROP TABLE spqr_metadata.spqr_distributed_relations;
DROP TABLE spqr_metadata.spqr_reference_relations;

CREATE TABLE spqr_metadata.spqr_distributed_relations (
    reloid OID PRIMARY KEY
);

CREATE TABLE spqr_metadata.spqr_reference_relations (
    reloid OID PRIMARY KEY
);
