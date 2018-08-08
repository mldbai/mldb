# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

ifdef asd

# Test for the presence of ~/.cloud_credentials with an S3 entry.  If present,
# some extra tests can be enabled.  This will be "1" or empty for true or false.
HAS_S3_CREDENTIALS:=$(shell grep -l "^s3" ~/.cloud_credentials >/dev/null 2>/dev/null && echo "1")

# Make a test manual if there are no S3 credentials available
MANUAL_IF_NO_S3:=$(if $(HAS_S3_CREDENTIALS),,manual)

$(eval $(call test,MLDB-889_nan_cellvalue,mldb mldb_pro_plugin,boost))

# Include the pro plugin, as we also want to test its documentation
$(TESTS)/mldb_internal_plugin_doc_test: $(MLDB_PLUGIN_FILES_pro)

$(eval $(call mldb_unit_test,MLDB-987-beh-time-quantization-test.py,pro))
$(eval $(call mldb_unit_test,MLDB-998-get-timestamp-range.js,pro))
$(eval $(call mldb_unit_test,MLDB-825-live-dataset.js,pro))
$(eval $(call mldb_unit_test,MLDB-460-disallow-empty-name.py,pro))
$(eval $(call mldb_unit_test,MLDB-826-vector-ops.js,pro))
$(eval $(call mldb_unit_test,MLDB-525-procedure-training-timestamp.js,pro))
$(eval $(call mldb_unit_test,MLDB-676-beh-files-http.js,pro))
$(eval $(call mldb_unit_test,MLDB-663_repeatable_svd.py,pro))
$(eval $(call mldb_unit_test,MLDB-563-beh-relative-paths.js,pro))
$(eval $(call mldb_unit_test,beh-binary-mutable-save-test.py,pro))
$(eval $(call mldb_unit_test,MLDB-1164-missing_columns.py,pro,manual)) # manual until we recover the test file
$(eval $(call mldb_unit_test,MLDB-1161-s3_upload_test.py,pro,$(MANUAL_IF_NO_S3)))
$(eval $(call mldb_unit_test,MLDB-1196-assert-failure.py,pro,manual)) # manual until we recover the test file
$(eval $(call mldb_unit_test,MLDB-1254-overwrite_file_test.py,pro))
$(eval $(call mldb_unit_test,MLDB-1279-communitysift-performance.py,pro,manual))
$(eval $(call mldb_unit_test,MLDBFB-307-svd-speed-test.py,pro,manual)) # manual until we recover the test file and fix it
$(eval $(call mldb_unit_test,MLDBFB-320-bits_tbits_assert_fail.py,pro))
$(eval $(call test,mldb_mnist_test,mldb,boost))
$(eval $(call mldb_unit_test,MLDB-696_uri_causes_crash.py))
$(eval $(call mldb_unit_test,MLDBFB-401_where_on_unexisting_col_test.py,pro))

$(eval $(call mldb_unit_test,MLDBFB-323-beh-limit.py,pro))
$(eval $(call mldb_unit_test,MLDBFB-404-case_into_beh_test.py,pro))
$(eval $(call mldb_unit_test,MLDBFB-422_sql_invalid_count_issue.py,pro))
$(eval $(call mldb_unit_test,MLDBFB-474-too-long-to-split.py,pro,$(MANUAL_IF_NO_S3)))
$(eval $(call mldb_unit_test,MLDBFB-505_mldb_query_json_error.py,pro,manual))
$(eval $(call mldb_unit_test,MLDB-1645_list_files_test.py,pro))
$(eval $(call mldb_unit_test,MLDBFB-530_continuous_window_fails_on_restart.py,pro))
$(eval $(call mldb_unit_test,MLDB-1669_get_dataset_stats_test.py,pro))
$(eval $(call mldb_unit_test,MLDBFB-545-incorrect_result_on_merged_ds.py,pro,manual))
$(eval $(call mldb_unit_test,beh_type_check_on_load_test.py,pro))
$(eval $(call mldb_unit_test,reddit_benchmark.py,pro,manual))
$(eval $(call mldb_unit_test,stackoverflow_benchmark.py,pro,manual))

endif
