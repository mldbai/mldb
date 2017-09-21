# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# Language support for MLDB

#$(eval $(call library,mldb_lang_plugins,,mldb_js_plugin))
$(eval $(call library,mldb_lang_plugins,,mldb_js_plugin mldb_python_plugin mldb_opencl_plugin))
$(eval $(call library_forward_dependency,mldb_lang_plugins,mldb_js_plugin mldb_python_plugin mldb_opencl_plugin))


$(eval $(call include_sub_make,mldb_js_plugin,js))
$(eval $(call include_sub_make,mldb_python_plugin,python))
$(eval $(call include_sub_make,opencl))
