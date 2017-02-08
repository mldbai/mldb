# sql_testing.mk
# Jeremy Barnes, 10 April 2016
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call test,path_test,sql_types,boost valgrind))
$(eval $(call test,path_benchmark,sql_types,boost))
$(eval $(call test,eval_sql_test,sql_expression,boost))
