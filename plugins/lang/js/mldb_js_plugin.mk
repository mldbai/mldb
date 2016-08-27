# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

LIBJS_LINK := jsoncpp $(V8_LIB) arch utils types

# JS plugin loader

JS_PLUGIN_SOURCE := \
	js_utils.cc \
	js_value.cc \
	js_loader.cc \
	js_function.cc \
	js_common.cc \
	dataset_js.cc \
	function_js.cc \
	procedure_js.cc \
	mldb_js.cc \


$(eval $(call set_compile_option,$(JS_PLUGIN_SOURCE),-I$(INC)))

$(eval $(call library,mldb_js_plugin,$(JS_PLUGIN_SOURCE),value_description $(LIBJS_LINK) sql_expression))
