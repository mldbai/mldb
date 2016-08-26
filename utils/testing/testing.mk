# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

LIB_TEST_UTILS_SOURCES := \
        benchmarks.cc \
        fixtures.cc \
        threaded_test.cc

LIB_TEST_UTILS_LINK := \
	arch utils boost_filesystem

$(eval $(call library,test_utils,$(LIB_TEST_UTILS_SOURCES),$(LIB_TEST_UTILS_LINK)))


$(eval $(call test,json_diff_test,json_diff vfs,boost))
$(eval $(call test,json_hash_test,json_diff,boost))
$(eval $(call test,command_expression_test,command_expression test_utils,boost))
$(eval $(call test,config_test,config,boost))
$(eval $(call test,logger_test,log,boost))
$(eval $(call test,compact_vector_test,arch,boost))
$(eval $(call test,fixture_test,test_utils,boost))
$(eval $(call test,print_utils_test,,boost))
