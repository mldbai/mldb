# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

# Test for the presence of ~/.cloud_credentials with an S3 entry.  If present,
# some extra tests can be enabled.  This will be "1" or empty for true or false.
# these tests access *public* files in the dev.mldb.datacratic.com bucket, so any old
# valid credentials will do
HAS_S3_CREDENTIALS:=$(shell grep -l "^s3" ~/.cloud_credentials >/dev/null 2>/dev/null && echo "1")

# Make a test manual if there are no S3 credentials available
MANUAL_IF_NO_S3:=$(if $(HAS_S3_CREDENTIALS),,manual)

$(eval $(call include_sub_make,cookbook))

#$(warning HAS_S3_CREDENTIALS=$(HAS_S3_CREDENTIALS))
#$(warning MANUAL_IF_NO_S3=$(MANUAL_IF_NO_S3))

$(eval $(call library,mldb_test_function,test_function.cc,mldb))


$(eval $(call mldb_unit_test,MLDBFB-336-sample_test.py,,manual))


$(eval $(call test,mldb_plugin_test,mldb,boost))
$(eval $(call test,mldb_python_plugin_test,mldb,boost))
$(eval $(call test,MLDB-642_script_procedure_test,mldb,boost))
$(eval $(call test,for_each_line_test,mldb,boost))
$(eval $(call test,svd_utils_test,mldb,boost))

$(eval $(call test,mldb_reddit_test,mldb,boost))
$(eval $(call test,cell_value_test,sql_expression,boost))

# NOTE: sql_expression_test should NOT depend on the MLDB library.  If you
# are tempted to add it, you have coupled them together and broken
# encapsulation.  Look at sql_expression_ops.cc for some ideas of how to
# re-decouple them.
$(eval $(call test,sql_expression_test,sql_expression,boost))
$(eval $(call test,dataset_select_test,mldb,boost))
$(eval $(call test,embedding_dataset_test,mldb,boost))
$(eval $(call test,procedure_run_test,mldb,boost))
$(eval $(call test,python_procedure_test,mldb,boost manual)) #manual -- unclear why
$(eval $(call test,mldb_internal_plugin_doc_test,mldb,boost))



$(eval $(call test,mldb_config_persistence_test,mldb,boost manual)) #this code will be removed as part of MLDB-1441
$(eval $(call test,mldb_startup_test,mldb,boost))
$(eval $(call test,mldb_plugin_delete_test,mldb,boost))
$(eval $(call test,pyplugin_static_folder_test,mldb,boost))
$(eval $(call test,mldb_function_delete_test,mldb mldb_test_function,boost))
$(eval $(call test,MLDB-204-circular-references-initialization,mldb,boost))
$(eval $(call test,MLDB-267-delete-while-loading,mldb,boost))
$(eval $(call test,mldb_crash_multiple_py_routes,mldb,boost manual))  #manual - intermittent - MLDB-787
$(eval $(call test,mldb_function_pin_test,mldb,boost))
$(eval $(call test,mldb_determinism_test,mldb,boost))
$(eval $(call test,credentials_daemon_test,credentials_daemon cloud,boost))
$(eval $(call test,MLDB-1025-output-dataset-serialization-test,mldb,boost))

$(TESTS)/credentials_daemon_test: $(BIN)/credentialsd


$(eval $(call mldb_unit_test,test_the_tester.js))
$(eval $(call mldb_unit_test,test_the_tester.py))
$(eval $(call mldb_unit_test,MLDB-119-select_no_columns.js))
$(eval $(call mldb_unit_test,MLDB-153_pyscript_state_pollution.py))
$(eval $(call mldb_unit_test,MLDB-162-min-max-function.py))
$(eval $(call mldb_unit_test,mldb_auto_id_test.js))
$(eval $(call mldb_unit_test,mldb_create_error_test.js))
$(eval $(call mldb_unit_test,MLDB-195-column-left-multiplication.js))
$(eval $(call mldb_unit_test,MLDB-59-probabilizer.js))
$(eval $(call mldb_unit_test,MLDB-238-sorting-missing-values.js))
$(eval $(call mldb_unit_test,MLDB-198-classifier-weights.js))
$(eval $(call mldb_unit_test,MLDB-173-multiclass.js))
$(eval $(call mldb_unit_test,MLDB-174-regression.py))
$(eval $(call mldb_unit_test,MLDB-255-type-routes.js))
$(eval $(call mldb_unit_test,MLDB-40-sparse-continuous-svd.js))
$(eval $(call mldb_unit_test,MLDB-299-post-sync.js))
$(eval $(call mldb_unit_test,MLDB-285-kmeans-function.js))
$(eval $(call mldb_unit_test,MLDB-300_function_scope.py))
$(eval $(call mldb_unit_test,MLDB-775_hashbucket_feat_gen.py))
$(eval $(call mldb_unit_test,MLDB-775_train_with_hashbucket_feat.py))
$(eval $(call mldb_unit_test,MLDB-348-put-post-id.js))
$(eval $(call mldb_unit_test,MLDB-326-put-post-training-ids.js))
$(eval $(call mldb_unit_test,MLDB-327-sum-aggregate.js))
$(eval $(call mldb_unit_test,MLDB-409-transform-dataset.js))
$(eval $(call mldb_unit_test,MLDB-410-set-output-rowname.js))
$(eval $(call mldb_unit_test,MLDB-415-rawquery.js))
$(eval $(call mldb_unit_test,MLDB-312_cant_overwrite_dataset.py))
$(eval $(call mldb_unit_test,MLDB-302_record_rowname_int.py))
$(eval $(call mldb_unit_test,MLDB-297-can-use-datetime.py))
$(eval $(call mldb_unit_test,MLDB-256_accuracy_accepts_all_cls_modes.py))
$(eval $(call mldb_unit_test,python_mldb_log.py))
$(eval $(call mldb_unit_test,procedure_409_test.py))
$(eval $(call mldb_unit_test,MLDB-426_tsne_crash.py))
$(eval $(call mldb_unit_test,MLDB-412_glibc.py))
$(eval $(call mldb_unit_test,MLDB-444_python_perform_default_args.py))
$(eval $(call mldb_unit_test,MLDB-305-sync-async.js))
$(eval $(call mldb_unit_test,MLDB-251-invalid-script-host.js))
$(eval $(call mldb_unit_test,MLDB-429-classifier-empty-label.js))
$(eval $(call mldb_unit_test,MLDB-434-beh-dataset-nulls.js))
$(eval $(call mldb_unit_test,MLDB-301-commit-empty-dataset.js))
$(eval $(call mldb_unit_test,MLDB-283-embedding-nearest-neighbours.js))
$(eval $(call mldb_unit_test,MLDB-417-empty-svd.js))
$(eval $(call mldb_unit_test,MLDB-485-svd_embedRow_returns_zeroes.py))
$(eval $(call mldb_unit_test,MLDB-481-vp-tree-high-dimensional-cube.js))
$(eval $(call mldb_unit_test,MLDB-466-full-reddit.js,,manual)) #manual - slow
$(eval $(call mldb_unit_test,MLDB-489-svd-nonvarying-reals.js))
$(eval $(call mldb_unit_test,MLDB-462-transposed-dataset.js))
$(eval $(call mldb_unit_test,MLDB-461_horizontal_ops_test.py))
$(eval $(call mldb_unit_test,MLDB-494-stream-destructor-hang.js))
$(eval $(call mldb_unit_test,MLDB-487-scikit-learn-comparison.py,,manual)) #manual - slow
$(eval $(call mldb_unit_test,MLDB-498-svd-apply-function.js))
$(eval $(call mldb_unit_test,MLDB-497-get-config-type-info.js))
$(eval $(call mldb_unit_test,MLDB-505-in-expression.py))
$(eval $(call mldb_unit_test,MLDB-533-low-rank-continuous-svd.js))
$(eval $(call mldb_unit_test,MLDB-534-svd-function-column-errors.js))
$(eval $(call mldb_unit_test,MLDB-537-hang-on-put-error.js))
$(eval $(call mldb_unit_test,MLDB-102-select-formats.js))
$(eval $(call mldb_unit_test,MLDB-543-column-named-zero.js))
$(eval $(call mldb_unit_test,MLDB-574-sqlite-backend.js))
$(eval $(call mldb_unit_test,MLDB-541-record-column.js))
$(eval $(call mldb_unit_test,MLDB-581-multiple-select.js))
$(eval $(call mldb_unit_test,MLDB-529-duplicate-pin.js))
$(eval $(call mldb_unit_test,MLDB-284-tsne-reapply.js,,manual))  #manual - waiting for fix
$(eval $(call mldb_unit_test,MLDB-592-bs-training-failure.py)) 
$(eval $(call mldb_unit_test,MLDB-390-sql-expression-function.js))
$(eval $(call mldb_unit_test,MLDB-593-serial-function-with-extract.js))
$(eval $(call mldb_unit_test,MLDB-587-empty-classifier.js))
$(eval $(call mldb_unit_test,MLDB-620-nonexistant-dataset-messages.js))
$(eval $(call mldb_unit_test,MLDB-572-error-procedure-config.js))

$(eval $(call mldb_unit_test,plugin_delete_test.py,,manual)) #manual - unclear reason for existence
$(eval $(call mldb_unit_test,MLDB-573_explain_function_floats.py))
$(eval $(call mldb_unit_test,MLDB-618_rowcol_named_0.py))
$(eval $(call mldb_unit_test,MLDB-619_newlines_in_sql.py))
$(eval $(call mldb_unit_test,MLDB-647_multiclass_apply_function_pins.py))
$(eval $(call mldb_unit_test,MLDB-665_nearest_neighbours.py))
$(eval $(call mldb_unit_test,MLDB-538_route_deadlock.py))
$(eval $(call mldb_unit_test,MLDB-643_script_function.py))
$(eval $(call mldb_unit_test,MLDB-668-object-column-name.py))
$(eval $(call mldb_unit_test,MLDB-621_serial_procedure_deadlock.py))
$(eval $(call mldb_unit_test,MLDB-654-classifier-function-info.js))
$(eval $(call mldb_unit_test,MLDB-605-timestamp-query.js))
$(eval $(call mldb_unit_test,MLDB-679-latest-get-variable.js))
$(eval $(call mldb_unit_test,MLDB-684-remote-plugin.js,,virtualenv))
$(eval $(call mldb_unit_test,MLDB-136-value-dataset.js))
$(eval $(call mldb_unit_test,MLDB-499-text-dataset.js))
$(eval $(call mldb_unit_test,MLDB-694_external_python_procedure.py))
$(eval $(call mldb_unit_test,MLDB-704-jseval-row.js))
$(eval $(call mldb_unit_test,MLDB-723-jseval-exceptions.js))
$(eval $(call mldb_unit_test,MLDB-761-sub-queries.py))
$(eval $(call mldb_unit_test,MLDB-565-classifier-details.js))
$(eval $(call mldb_unit_test,MLDB-749-csv-dataset.js))
$(eval $(call mldb_unit_test,MLDB-702-row-aggregators.py))
$(eval $(call mldb_unit_test,MLDB-770-group-by-parsing.js))
$(eval $(call mldb_unit_test,MLDB-768-order-by-missing-function.js))
$(eval $(call mldb_unit_test,MLDB-180-basic-join.js))
$(eval $(call mldb_unit_test,MLDB-779_cant_test_bs_cls.py))
$(eval $(call mldb_unit_test,MLDB-703-count-not-null.js))
$(eval $(call mldb_unit_test,MLDB-781-numeric-functions.js))
$(eval $(call mldb_unit_test,MLDB-785-decision-tree-missing.js))
$(eval $(call mldb_unit_test,MLDB-784_sqlquery_join.py))
$(eval $(call mldb_unit_test,MLDB-788_rowname_date_in_transform.py))
$(eval $(call mldb_unit_test,MLDB-797-nested_sql_expressions.py))
$(eval $(call mldb_unit_test,MLDB-800-nested_sql_query.py))
$(eval $(call mldb_unit_test,MLDB-801-from-table-expression.js))
$(eval $(call mldb_unit_test,MLDB-804-null-accuracy.js))
$(eval $(call mldb_unit_test,MLDB-805-apply_func_svd_embed.py))
$(eval $(call mldb_unit_test,MLDB-809-group-by-rowname.js))
$(eval $(call mldb_unit_test,MLDB-813-rowname_in_join.py))
$(eval $(call mldb_unit_test,MLDB-724-time-arithmetic.py))
$(eval $(call mldb_unit_test,MLDB-816-scatter-aggregate.js))
$(eval $(call mldb_unit_test,MLDB-798-sql-operation-precision.py))
$(eval $(call mldb_unit_test,MLDB-832-select_star.py))
$(eval $(call mldb_unit_test,MLDB-835-table-aliases.py))
$(eval $(call mldb_unit_test,MLDB-861-character-encoding.py))
$(eval $(call mldb_unit_test,MLDB-865-javascript-unicode.js))
$(eval $(call mldb_unit_test,MLDB-869-select-expression.py))
$(eval $(call mldb_unit_test,MLDB-871-json-non-ascii-keys.js))
$(eval $(call mldb_unit_test,MLDB-873_stats_table_test.py))
$(eval $(call mldb_unit_test,MLDB-878_experiment_proc.py))
$(eval $(call mldb_unit_test,MLDB-894_runs_can_override_conf.py))
$(eval $(call mldb_unit_test,MLDB-917_replace_nan_inf.py))
$(eval $(call test,MLDB-896_table_expression_serialised_as_string,mldb,boost))
$(eval $(call test,MLDB-991_permuter_permutations,mldb,boost))
$(eval $(call mldb_unit_test,MLDB-558-python-unicode.py))
$(eval $(call mldb_unit_test,MLDB-756-docker-plugin.js,, manual)) #Manual because there are issues with keeping the .so up to date
$(eval $(call mldb_unit_test,MLDB-991_permuter_procedure.py))
$(eval $(call mldb_unit_test,MLDB-881-DELETE-fails-on-in-construction.py))
$(eval $(call mldb_unit_test,MLDB-909-simple-WHEN-expression.py))
$(eval $(call mldb_unit_test,MLDB-895-fuzz-cases.py))
$(eval $(call mldb_unit_test,MLDB-900-archives.js))
$(eval $(call mldb_unit_test,MLDB-905-docker-archive.js))
$(eval $(call mldb_unit_test,MLDB-926_auto_functions_for_procs.py))
$(eval $(call mldb_unit_test,MLDB-902-non-ascii-row-names.py))
$(eval $(call mldb_unit_test,MLDB-906-date-function.py))
$(eval $(call mldb_unit_test,MLDB-907-tokenize.py))
$(eval $(call mldb_unit_test,MLDB-915-pivot-transform.py))
$(eval $(call mldb_unit_test,MLDB-945-WHEN-in-proc-and-func.py))
$(eval $(call mldb_unit_test,MLDB-923-embedding-literal.py))
$(eval $(call mldb_unit_test,MLDB-953-normalize.py))
$(eval $(call mldb_unit_test,MLDB-956-sql-comments.py))
$(eval $(call mldb_unit_test,MLDB-957-function-name.py))
$(eval $(call mldb_unit_test,MLDB-961-glz-categorical.js))
$(eval $(call mldb_unit_test,MLDB-973-csv-linenumber.js))
$(eval $(call mldb_unit_test,MLDB-963-when-in-WHEN.py))
$(eval $(call mldb_unit_test,MLDB-927-null-row-output.py))
$(eval $(call mldb_unit_test,MLDB-1012_nested_function_calls.py))
$(eval $(call mldb_unit_test,MLDB-983-create-dataset-ids.js))
$(eval $(call mldb_unit_test,MLDB-985-create-entity-return-details.js))
$(eval $(call mldb_unit_test,MLDB-986-recording.py))
$(eval $(call mldb_unit_test,MLDB-991-svm.py))
$(eval $(call mldb_unit_test,MLDB-995-sub-query-sparse.js))
$(eval $(call mldb_unit_test,MLDB-1116-tokensplit.py))
$(eval $(call mldb_unit_test,MLDB-1030_apply_stopwords.py))
$(eval $(call mldb_unit_test,MLDB-1165-where-rowname-in-optim.py,,manual)) # based on perf of specific machine
$(eval $(call mldb_unit_test,MLDB-1304-titanic-demo.py))

$(eval $(call mldb_unit_test,MLDB-1000-type-documentation-valid.js))
$(eval $(call mldb_unit_test,MLDB-1003-s3-load-dataset.js,,$(MANUAL_IF_NO_S3)))

$(eval $(call mldb_unit_test,MLDB-1010-put-no-payload-error.js))

$(eval $(call mldb_unit_test,MLDB-1019-word2vec.js,,manual)) # manual---requires large local data file
$(eval $(call mldb_unit_test,MLDB-1084_sentiwordnet.py,,$(MANUAL_IF_NO_S3)))
$(eval $(call mldb_unit_test,MLDB-1101-tf-idf.py))
$(eval $(call mldb_unit_test,MLDB-1117-git-import.js))
$(eval $(call mldb_unit_test,MLDB-1120-sparse-mutable-values.js))
$(eval $(call mldb_unit_test,MLDB-1142-sparse-mutable-failing-with-underflow.py))

$(eval $(call mldb_unit_test,MLDB-1033-sparse-timestamp-interval.js))
$(eval $(call mldb_unit_test,MLDB-1026-slow-not-in.js))
$(eval $(call mldb_unit_test,MLDB-974-slow-subquery.js))
$(eval $(call mldb_unit_test,MLDB-1155_csv_line_endings.py))
$(eval $(call test,MLDB-1040-invalid-requests,mldb,boost))
$(eval $(call mldb_unit_test,MLDB-1081-getEmbedding_honors_limit_offset.py))
$(eval $(call mldb_unit_test,MLDB-951-run-on-creation.py))
$(eval $(call mldb_unit_test,MLDB-1092_conf_interval.py))
$(eval $(call test,MLDB-1092_confidence_intervals_test,mldb,boost))
$(eval $(call mldb_unit_test,MLDB-1043-bucketize-procedure.js))
$(eval $(call mldb_unit_test,MLDB-1025-dataset-output-with-default.py))
$(eval $(call mldb_unit_test,MLDB-989-complex-order-by.py))
$(eval $(call mldb_unit_test,MLDB-1126_stemming.py))
$(eval $(call mldb_unit_test,MLDB-1127-order-by-and-where-in-svd.py))
$(eval $(call mldb_unit_test,MLDB-284-tsne-apply-function.py))
$(eval $(call mldb_unit_test,MLDB-1119_pooling_function.py))
$(eval $(call mldb_unit_test,MLDB-1172_column_expr_fail.py))
$(eval $(call mldb_unit_test,MLDB-1104-input-data-spec.py))
$(eval $(call mldb_unit_test,MLDB-1190_segfault_sqlexpr_jseval.py))
$(eval $(call mldb_unit_test,MLDB-1198-sum-inconsistency-test.py))
$(eval $(call mldb_unit_test,MLDB-1242_sampled_dataset.py))
$(eval $(call mldb_unit_test,MLDB-1266-import_json.py))
$(eval $(call mldb_unit_test,MLDB-1258_nofrom_segfault.py))
$(eval $(call mldb_unit_test,MLDB-1212_csv_import_long_quoted_lines.py))
$(eval $(call mldb_unit_test,MLDB-1275_melt_procedure.py))
$(eval $(call mldb_unit_test,MLDB-1273-classifier-row_input.py))
$(eval $(call mldb_unit_test,MLDB-1305_rowNames_join.py))
$(eval $(call mldb_unit_test,MLDB-1272-regression-training-failure.py))
$(eval $(call mldb_unit_test,MLDB-1277-pooling-performance.py,,manual)) #manual -- awaiting fix
$(eval $(call mldb_unit_test,MLDB-1235-temporal-aggregators.py))
$(eval $(call mldb_unit_test,MLDB-1260-json-errors.py))
$(eval $(call mldb_unit_test,MLDB-1239-utf8-literal.py))
$(eval $(call mldb_unit_test,MLDB-1353-EM.py))
$(eval $(call mldb_unit_test,MLDB-1361_join_on_subselect.py))
$(eval $(call mldb_unit_test,MLDB-1364_dataset_cant_be_overwritten.py))
$(eval $(call mldb_unit_test,MLDB-1336-builtin-checks.py))
$(eval $(call mldb_unit_test,MLDB-1433-random-forest.py))
$(eval $(call mldb_unit_test,MLDB-1430-aggregate-bug.py))
$(eval $(call mldb_unit_test,MLDB-1428-text-sparse-output.py))
$(eval $(call mldb_unit_test,MLDB-1452-like-operator.py))
$(eval $(call mldb_unit_test,MLDB-1440_sqlexpr_ignore_unknown_param.py))

$(eval $(call mldb_unit_test,pytanic_plugin_test.py))
$(eval $(call python_test,mldb_merged_dataset_test,mldb_py_runner))

pytanic_plugin_test mldb_recordrow_test_py pytanic_test mldb_dataset_test_py null_column_test dcframe_test reddit_example_test pytanic_test mldb_merged_test $(BIN)/mldb_py_runner/__init__.py: \
	$(BIN)/mldb_runner


$(eval $(call python_test,mldb_dataset_test_py, mldb_py_runner))
$(eval $(call python_test,mldb_recordrow_test_py,mldb_py_runner))
$(eval $(call python_test,null_column_test,mldb_py_runner))

$(eval $(call python_test,mldb-417_svd,mldb_py_runner))

$(eval $(call include_sub_make,mldb_py_runner))

$(eval $(call python_addon,py_conv_test_module,python_converters_test_support.cc,mldb_python_plugin python2.7 boost_python types arch mldb))

$(eval $(call python_test,python_converters_test,py_conv_test_module))

$(eval $(call python_addon,py_cell_conv_test_module,python_cell_converter_test_support.cc,python2.7 boost_python types mldb services))

$(eval $(call python_test,python_cell_converter_test,py_cell_conv_test_module))

$(eval $(call mldb_unit_test,MLDB-1011-excel-import.js))
$(eval $(call mldb_unit_test,MLDB-1121-csv-import-duplicates.py))
$(eval $(call mldb_unit_test,MLDB-1098-csv-export.py))
$(eval $(call mldb_unit_test,MLDB-1098-csv-export-advanced.py))

$(eval $(call mldb_unit_test,MLDB-1128-transform-utf8.js,,manual)) #manual -- requires specific local file?
$(eval $(call mldb_unit_test,MLDB-1140-csv_reading_compression_test.py))
$(eval $(call mldb_unit_test,MLDB-980-unquoted-string-crash.js))
$(eval $(call mldb_unit_test,MLDB-1195-query-where-test.py))
$(eval $(call mldb_unit_test,MLDB-1192-js-procedure-function.js))
$(eval $(call mldb_unit_test,MLDB-1253_concat_test.py))
$(eval $(call mldb_unit_test,MLDBFB-308-where-outer-join-test.py,,manual)) #manual -- awaiting fix
$(eval $(call mldb_unit_test,MLDB-1267-bucketize-ts-test.py))
$(eval $(call mldb_unit_test,MLDB-1322-sum_stem_token.py))
$(eval $(call mldb_unit_test,python_mldb_interface_test.py))
$(eval $(call mldb_unit_test,MLDB-1319-new-executor-function-binding.js))
$(eval $(call mldb_unit_test,MLDB-1328-join_empty_dataset_test.py))
$(eval $(call mldb_unit_test,MLDB-1213-blob-support.js))
$(eval $(call mldb_unit_test,MLDBFB-332-transform_input_sum_doesnt_exist_test.py))
$(eval $(call mldb_unit_test,MLDB-1320-sql-query-whole-table.js))
$(eval $(call mldb_unit_test,MLDB-1315-row-table-expressions.js))
$(eval $(call mldb_unit_test,MLDB-1323-complicated-query.py,,manual)) # slow and depends on iffy file
$(eval $(call mldb_unit_test,MLDB-1345-having.py))
$(eval $(call mldb_unit_test,mldb_unit_test_test.py))
$(eval $(call mldb_unit_test,MLDB-1216-fetcher-function.js))
$(eval $(call mldb_unit_test,MLDBFB-335_when_timestamp_variable_test.py))
$(eval $(call mldb_unit_test,MLDBFB-192_row_name_as_string_test.py))
$(eval $(call mldb_unit_test,MLDBFB-199_invalid_script_test.py))
$(eval $(call mldb_unit_test,MLDBFB-208_procedure_params_overwrite_test.py))
$(eval $(call mldb_unit_test,MLDB-687-svd-embed-row-single-column.js))
$(eval $(call mldb_unit_test,MLDB-1359_procedure_latest_run.py))
$(eval $(call mldb_unit_test,MLDBFB-345_improve_error_message_named_on_null.py))
$(eval $(call mldb_unit_test,MLDB-1317-tensor-datatype.js))
$(eval $(call mldb_unit_test,get_http_bound_address.py))
$(eval $(call mldb_unit_test,get_http_bound_address.js))
$(eval $(call mldb_unit_test,MLDB-815-sparse-mutable-record-strings.js))
$(eval $(call mldb_unit_test,MLDB-1395-error-message-file-doesnt-exist.js))
$(eval $(call mldb_unit_test,ranking_test.py))
$(eval $(call mldb_unit_test,MLDB-1490-grouped-validation.py))
$(eval $(call mldb_unit_test,MLDB-1491-get-all-not-implemented-for-datasets.js,,manual)) # awaiting fix
$(eval $(call mldb_unit_test,MLDB-1500-transpose-query.js,,manual)) # awaiting fix

# The MLDB-1398 test case requires a library and a plugin
# Tensorflow plugins
$(eval $(call include_sub_make,MLDB-1398-plugin))
$(eval $(call mldb_unit_test,MLDB-1398-plugin-library-dependency.js,MLDB-1398-plugin))
$(eval $(call mldb_unit_test,MLDB-1554-string-agg.js))

$(eval $(call test,MLDB-1360-sparse-mutable-multithreaded-insert,mldb,boost))

