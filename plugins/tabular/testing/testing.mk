# sql_testing.mk
# Jeremy Barnes, 10 April 2016
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call test,transducer_test,mldb_tabular_plugin utils,boost))
