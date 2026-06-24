# contrib/spqrguard/Makefile

MODULE_big	= spqrguard
OBJS = \
	$(WIN32RES) \
	spqrguard.o

EXTENSION = spqrguard
DATA =  spqrguard--1.0.sql spqrguard--1.0--2.0.sql \
		 spqrguard--1.0--2.1.sql spqrguard--2.0.sql \
		 spqrguard--2.0--2.1.sql spqrguard--2.1.sql \
		 spqrguard--2.1--2.2.sql spqrguard--2.2--2.3.sql \
		 spqrguard--2.3--2.4.sql spqrguard--2.4--2.4.1.sql

PGFILEDESC = "spqrguard - module for asserting SPQR data integrity"

REGRESS = simple global_settings versions key_ranges
ISOLATION = drop_extension
ISOLATION_OPTS = --load-extension=spqrguard

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
