set allow_system_table_mods to true;
CREATE EXTENSION spqrguard;

CREATE TABLE not_spqr_t(i INT);
CREATE TABLE spqr_d_t(i INT);
CREATE TABLE spqr_ref_t(i INT);

SELECT spqr_metadata.mark_distributed_relation ('spqr_d_t');
SELECT spqr_metadata.mark_reference_relation ('spqr_ref_t');


INSERT INTO not_spqr_t VALUES (1);
INSERT INTO spqr_d_t VALUES (1);
INSERT INTO spqr_ref_t VALUES (1);
SET spqrguard.prevent_distributed_table_modify TO true;

--INSERT INTO spqr_metadata.spqr_global_settings VALUES (42, 'on');

INSERT INTO not_spqr_t VALUES (1);
INSERT INTO spqr_d_t VALUES (1);
INSERT INTO spqr_ref_t VALUES (1);

DROP TABLE not_spqr_t, spqr_d_t, spqr_ref_t;

DROP EXTENSION spqrguard;
