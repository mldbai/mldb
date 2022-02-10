# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call test,metal_reference_counting_test,vfs block types value_description vfs utils base arch command_expression mldb_metal_plugin any,boost))
