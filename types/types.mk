# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# date.mk
# Jeremy Barnes, 2 November 2011
# Copyright (c) 2011 mldb.ai inc.  All rights reserved.
#
# Date library for MLDB.

LIBVALUE_DESCRIPTION_SOURCES := \
	value_description.cc \
	basic_value_descriptions.cc \
	libc_value_descriptions.cc \
	json_parsing.cc \
	json_printing.cc \
	dtoa.c \
	meta_value_description.cc \
	distribution_description.cc

LIBVALUE_DESCRIPTION_LINK := \
	arch jsoncpp base

$(eval $(call library,value_description,$(LIBVALUE_DESCRIPTION_SOURCES),$(LIBVALUE_DESCRIPTION_LINK)))

$(eval $(call library,any,any.cc,value_description))


LIBTYPES_SOURCES := \
	date.cc \
	localdate.cc \
	string.cc \
	url.cc \
	periodic_utils.cc \
	dtoa.c \
	regex.cc \
	periodic_utils_value_descriptions.cc

LIBTYPES_LINK := \
	rt boost_locale boost_regex boost_date_time jsoncpp googleurl cityhash value_description

$(eval $(call set_compile_option,localdate.cc,-DLIB=\"$(LIB)\"))

ifneq ($(PREMAKE),1)
$(LIB)/libtypes.so: $(LIB)/date_timezone_spec.csv

$(LIB)/date_timezone_spec.csv: $(CWD)/date_timezone_spec.csv $(LIB)/.dir_exists
	@echo "           $(COLOR_CYAN)[COPY]$(COLOR_RESET) $< -> $@"
	@/bin/cp -f $< $@
endif

$(eval $(call library,types,$(LIBTYPES_SOURCES),$(LIBTYPES_LINK)))


$(eval $(call include_sub_make,types_testing,testing,types_testing.mk))
