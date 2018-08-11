# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

LIBDB_SOURCES := \
        compact_size_types.cc \
        nested_archive.cc \
        portable_iarchive.cc \
        portable_oarchive.cc

$(eval $(call add_sources,$(LIBDB_SOURCES)))

LIBDB_LINK := utils

$(eval $(call library,db,$(LIBDB_SOURCES),$(LIBDB_LINK)))

$(eval $(call include_sub_make,db_testing,testing))
