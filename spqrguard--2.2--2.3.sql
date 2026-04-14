-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION spqrguard" to load this file. \quit

CREATE TABLE spqr_metadata.spqr_transferred_reference_relations (
    reloid OID PRIMARY KEY
);

CREATE FUNCTION
spqr_metadata.mark_transferred_reference_relation (reloid OID) 
RETURNS VOID AS
$$
INSERT INTO spqr_metadata.spqr_transferred_reference_relations VALUES (reloid);
$$
LANGUAGE SQL;

CREATE FUNCTION
spqr_metadata.mark_transferred_reference_relation (relname TEXT)
RETURNS VOID AS
$$
SELECT spqr_metadata.mark_transferred_reference_relation((relname)::regclass::oid);
$$
LANGUAGE SQL;

CREATE FUNCTION
spqr_metadata.unmark_transferred_reference_relation (reloid OID)
RETURNS VOID AS
$$
DELETE FROM spqr_metadata.spqr_transferred_reference_relations WHERE spqr_transferred_reference_relations.reloid = unmark_transferred_reference_relation.reloid;
$$
LANGUAGE SQL;

CREATE FUNCTION
spqr_metadata.unmark_transferred_reference_relation (relname TEXT)
RETURNS VOID AS
$$
SELECT spqr_metadata.unmark_transferred_reference_relation((relname)::regclass::oid);
$$
LANGUAGE SQL;
