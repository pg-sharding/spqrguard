setup
{
    CREATE TABLE not_spqr_t(i INT);
    CREATE TABLE spqr_d_t(i INT);
    CREATE TABLE spqr_ref_t(i INT);
}

teardown
{
    DROP TABLE not_spqr_t;
    DROP TABLE spqr_d_t;
    DROP TABLE spqr_ref_t;
}


session s1
step s1_insert_not_spqr { INSERT INTO not_spqr_t(i) VALUES (1); }
step s1_insert_d { INSERT INTO spqr_d_t(i) VALUES (1); }
step s1_insert_ref { INSERT INTO spqr_ref_t(i) VALUES (1); }
step s1_select_not_spqr {SELECT count(*) FROM not_spqr_t; }
step s1_select_d {SELECT count(*) FROM spqr_d_t; }
step s1_select_ref {SELECT count(*) FROM spqr_ref_t; }

session s2
step s2_drop_extension { DROP EXTENSION spqrguard; }

permutation s1_insert_not_spqr s1_insert_d s1_insert_ref s2_drop_extension s1_insert_not_spqr s1_insert_d s1_insert_ref s1_select_not_spqr s1_select_d s1_select_ref
