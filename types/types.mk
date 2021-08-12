# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# date.mk
# Jeremy Barnes, 2 November 2011
# Copyright (c) 2011 mldb.ai inc.  All rights reserved.
#
# Date library for MLDB.

LIBVALUE_DESCRIPTION_SOURCES := \
	string.cc \
	value_description.cc \
	basic_value_descriptions.cc \
	libc_value_descriptions.cc \
	json_parsing.cc \
	json_printing.cc \
	dtoa.c \
	meta_value_description.cc \
	distribution_description.cc \
	../ext/jsoncpp/json_reader.cpp \
	../ext/jsoncpp/json_writer.cpp \
	../ext/jsoncpp/json_value.cpp

LIBVALUE_DESCRIPTION_LINK := \
	arch base icui18n icuuc icudata

$(eval $(call library,value_description,$(LIBVALUE_DESCRIPTION_SOURCES),$(LIBVALUE_DESCRIPTION_LINK)))

$(eval $(call library,any,any.cc,value_description arch base))


LIBTYPES_SOURCES := \
	date.cc \
	localdate.cc \
	url.cc \
	periodic_utils.cc \
	dtoa.c \
	regex.cc \
	periodic_utils_value_descriptions.cc \
	path.cc \
	annotated_exception.cc


LIBTYPES_LINK := \
	$(LIBRT) boost_regex boost_date_time googleurl cityhash value_description highwayhash re2 any icui18n icuuc icudata base arch

$(eval $(call set_compile_option,string.cc,-I$(ICU_INCLUDE_PATH)))
$(eval $(call set_compile_option,regex.cc,-I$(RE2_INCLUDE_PATH) -I$(ICU_INCLUDE_PATH) -Wno-unused-variable))

ifneq ($(PREMAKE),1)
$(LIB)/libtypes$(SO_EXTENSION): $(LIB)/date_timezone_spec.csv

$(LIB)/date_timezone_spec.csv: $(CWD)/date_timezone_spec.csv $(LIB)/.dir_exists
	@echo "           $(COLOR_CYAN)[COPY]$(COLOR_RESET) $< -> $@"
	@/bin/cp -f $< $@
endif

$(eval $(call library,types,$(LIBTYPES_SOURCES),$(LIBTYPES_LINK)))


$(eval $(call include_sub_make,types_testing,testing,types_testing.mk))
$(eval $(call include_sub_make,db))
