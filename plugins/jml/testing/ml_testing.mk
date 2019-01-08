# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call test,bucketing_probabilizer_test,ml,boost))
$(eval $(call test,kmeans_test,ml test_utils,boost))
$(eval $(call test,configuration_test,jml_utils arch,boost))
$(eval $(call test,rf_kernel_test,mldb_jml_plugin test_utils,boost))
