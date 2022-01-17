# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call test,content_descriptor_test,vfs block types value_description vfs,boost))
$(eval $(call test,compute_kernel_test,vfs block types value_description vfs,boost))
$(eval $(call test,compute_promise_test,block types value_description,boost))
$(eval $(call test,compute_kernel_cpu_test,vfs block types value_description vfs utils base arch command_expression,boost))
