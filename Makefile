# contrib/spqrguard/Makefile

MODULE_big	= spqrguard
OBJS = \
	$(WIN32RES) \
	spqrguard.o

EXTENSION = spqrguard
DATA =  spqrguard--1.0.sql

PGFILEDESC = "spqrguard - module for asserting SPQR data integrity"

REGRESS = check

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/spqrguard
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
