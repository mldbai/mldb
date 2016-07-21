# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

# JS support library

LIBJS_SOURCES := \
	js_value.cc \
	js_utils.cc

LIBJS_LINK := jsoncpp $(V8_LIB) arch utils types

$(eval $(call library,js,$(LIBJS_SOURCES),$(LIBJS_LINK)))

$(eval $(call include_sub_make,js_testing,testing,js_testing.mk))

# JS plugin loader

$(eval $(call library,mldb_js_plugin,js_loader.cc js_function.cc js_common.cc mldb_js.cc dataset_js.cc function_js.cc procedure_js.cc,value_description js sql_expression))
