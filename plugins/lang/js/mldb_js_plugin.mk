# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

# JS plugin loader

$(eval $(call library,mldb_js_plugin,js_loader.cc js_function.cc js_common.cc mldb_js.cc dataset_js.cc function_js.cc procedure_js.cc,value_description js sql_expression))
