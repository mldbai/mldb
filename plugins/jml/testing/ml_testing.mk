# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call test,bucketing_probabilizer_test,ml arch value_description types algebra,boost))
$(eval $(call test,kmeans_test,ml arch value_description types test_utils,boost))
$(eval $(call test,configuration_test,ml arch value_description types utils,boost))
