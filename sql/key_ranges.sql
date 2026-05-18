CREATE EXTENSION spqrguard;

CREATE TABLE spqr_d_t(i INT);

INSERT INTO spqr_metadata.spqr_local_key_ranges (key_range_id, spqr_distribution, lower_bound) VALUES ('kr1', '', 0);

INSERT INTO spqr_d_t (i) VALUES (1);

SELECT spqr_metadata.lock_key_range_read('kr1', 0);

INSERT INTO spqr_d_t (i) VALUES (2);

SELECT spqr_metadata.lock_key_range_read('nonexistent', 0);

DROP EXTENSION spqrguard;
