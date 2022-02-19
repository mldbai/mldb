# sql_testing.mk
# Jeremy Barnes, 10 April 2016
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

TABULAR_TEST_LINK:=mldb_tabular_plugin utils block utils base arch value_description types vfs

$(eval $(call test,transducer_test,$(TABULAR_TEST_LINK),boost))

$(eval $(call test,int_table_test,$(TABULAR_TEST_LINK),catch2))
$(eval $(call test,int_table_real_world_test,$(TABULAR_TEST_LINK),catch2))
$(eval $(call test,bit_compressed_int_table_test,$(TABULAR_TEST_LINK),catch2))
$(eval $(call test,run_length_int_table_test,$(TABULAR_TEST_LINK),catch2))
$(eval $(call test,factored_int_table_test,$(TABULAR_TEST_LINK),catch2))
$(eval $(call test,cluster_test,$(TABULAR_TEST_LINK),catch2 manual))
$(eval $(call test,mapped_selector_table_test,$(TABULAR_TEST_LINK),catch2 manual))
$(eval $(call test,raw_mapped_int_table_test,$(TABULAR_TEST_LINK),catch2))
$(eval $(call test,mmap_test,$(TABULAR_TEST_LINK),catch2))
$(eval $(call test,fit_accumulator_test,$(TABULAR_TEST_LINK) algebra,catch2 manual))
