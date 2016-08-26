# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

LIBJS_LINK := jsoncpp $(V8_LIB) arch utils types

# JS plugin loader

$(eval $(call library,mldb_js_plugin,js_utils.cc js_value.cc js_loader.cc js_function.cc js_common.cc mldb_js.cc dataset_js.cc function_js.cc procedure_js.cc,value_description $(LIBJS_LINK) sql_expression))
