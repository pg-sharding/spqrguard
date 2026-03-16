SET allow_system_table_mods TO TRUE;
CREATE EXTENSION spqrguard VERSION '1.0';

ALTER EXTENSION spqrguard UPDATE TO '2.0';
ALTER EXTENSION spqrguard UPDATE TO '2.1';
ALTER EXTENSION spqrguard UPDATE TO '2.2';

DROP EXTENSION spqrguard;
