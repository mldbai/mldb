# js.mk
# Jeremy Barnes, 11 May 2010
# Copyright (c) 2010 mldb.ai inc.  All rights reserved.
#
# Support functions for javascript

LIBSQLITE_SOURCES := \
	sqlite3ppext.cc \
	sqlite3pp.cc \
	sqlite3.c

LIBSQLITE_LINK := 

# gcc 4.7 and above require this
$(eval $(call set_compile_option,sqlite3.c,-Wno-array-bounds -Wno-unused-const-variable))

$(eval $(call library,sqlite-mldb,$(LIBSQLITE_SOURCES),$(LIBSQLITE_LINK)))

$(eval $(call program,sqliteShell,sqlite-mldb,shell.c))
