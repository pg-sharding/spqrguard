CREATE EXTENSION spqrguard;

CREATE ROLE spqrguard_reader;

CREATE TABLE spqr_d_t(i INT);
SELECT spqr_metadata.mark_distributed_relation ('spqr_d_t');

SET ROLE spqrguard_reader;

SELECT count(*) FROM spqr_metadata.spqr_distributed_relations;
SELECT count(*) FROM spqr_metadata.spqr_local_key_ranges;

-- read-only: metadata modification is still denied
INSERT INTO spqr_metadata.spqr_distributed_relations VALUES (0);
SELECT spqr_metadata.unmark_distributed_relation ('spqr_d_t');

RESET ROLE;

SELECT spqr_metadata.unmark_distributed_relation ('spqr_d_t');
DROP TABLE spqr_d_t;
DROP ROLE spqrguard_reader;

DROP EXTENSION spqrguard;
