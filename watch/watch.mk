# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

LIBWATCH_SOURCES := \
	watch.cc \

$(eval $(call library,watch,$(LIBWATCH_SOURCES),gc any types arch base value_description))

$(eval $(call include_sub_make,watch_testing,testing))
