# contrib/spqrguard/Makefile

MODULE_big	= spqrguard
OBJS = \
	$(WIN32RES) \
	spqrguard.o

EXTENSION = spqrguard
DATA =  spqrguard--1.0.sql

PGFILEDESC = "spqrguard - module for asserting SPQR data integrity"

REGRESS = simple

TAP_TESTS = 1

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
