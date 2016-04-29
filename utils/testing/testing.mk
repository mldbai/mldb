# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

$(eval $(call test,json_diff_test,json_diff vfs,boost))
$(eval $(call test,json_hash_test,json_diff,boost))
$(eval $(call test,command_expression_test,command_expression test_utils,boost))
$(eval $(call test,config_test,config,boost))
$(eval $(call test,logger_test,log,boost))
