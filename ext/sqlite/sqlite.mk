# js.mk
# Jeremy Barnes, 11 May 2010
# Copyright (c) 2010 mldb.ai inc.  All rights reserved.
#
# Support functions for javascript

LIBDATACRATIC_SQLITE_SOURCES := \
	sqlite3ppext.cc \
	sqlite3pp.cc \
	sqlite3.c

LIBDATACRATIC_SQLITE_LINK := 

# gcc 4.7 and above require this
$(eval $(call set_compile_option,sqlite3.c,-Wno-array-bounds -Wno-unused-const-variable))

$(eval $(call library,datacratic_sqlite,$(LIBDATACRATIC_SQLITE_SOURCES),$(LIBDATACRATIC_SQLITE_LINK)))

$(eval $(call program,sqliteShell,datacratic_sqlite,shell.c))
