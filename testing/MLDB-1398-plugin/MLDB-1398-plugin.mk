# MLDB-1398-plugin.mk
# Jeremy Barnes, 22 February 2016
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
# Test plugin for MLDB-1398, which tests the ability to load a plugin with
# functionality implemented in multiple libraries.

$(eval $(call mldb_plugin_library,MLDB-1398-plugin,MLDB-1398-dep-library,MLDB-1398-dep-library.cc,))
$(eval $(call mldb_plugin_library,MLDB-1398-plugin,MLDB-1398-main-library,MLDB-1398-main-library.cc,MLDB-1398-dep-library))

$(eval $(call mldb_builtin_plugin,MLDB-1398-plugin,MLDB-1398-main-library))

