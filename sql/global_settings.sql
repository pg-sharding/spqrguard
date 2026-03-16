CREATE EXTENSION spqrguard;

CREATE TABLE not_spqr_t(i INT);
CREATE TABLE spqr_d_t(i INT);
CREATE TABLE spqr_ref_t(i INT);

SELECT spqr_metadata.mark_distributed_relation ('spqr_d_t');
SELECT spqr_metadata.mark_reference_relation ('spqr_ref_t');

INSERT INTO not_spqr_t VALUES (1);
INSERT INTO spqr_d_t VALUES (1);
INSERT INTO spqr_ref_t VALUES (1);

INSERT INTO spqr_metadata.spqr_global_settings (name, enabled) VALUES (42, true);

INSERT INTO not_spqr_t VALUES (2);
INSERT INTO spqr_d_t VALUES (2);

SET spqrguard.prevent_distributed_table_modify TO 'off';

INSERT INTO spqr_d_t VALUES (3);

SELECT spqr_metadata.unmark_distributed_relation ('spqr_d_t');

INSERT INTO spqr_d_t VALUES (4);

DELETE FROM spqr_metadata.spqr_global_settings WHERE name = 42;
INSERT INTO spqr_metadata.spqr_global_settings (name, enabled) VALUES (69, true);

INSERT INTO spqr_ref_t VALUES (2);

SET spqrguard.prevent_reference_table_modify TO 'off';

INSERT INTO spqr_ref_t VALUES (3);

SELECT spqr_metadata.unmark_reference_relation ('spqr_ref_t');

INSERT INTO spqr_ref_t VALUES (4);

DROP TABLE not_spqr_t, spqr_d_t, spqr_ref_t;

DROP EXTENSION spqrguard;
