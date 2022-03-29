# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

LIB_TEST_UTILS_SOURCES := \
        benchmarks.cc \
        fixtures.cc \
        threaded_test.cc

LIB_TEST_UTILS_LINK := \
	arch utils $(STD_FILESYSTEM_LIBNAME)

$(eval $(call library,test_utils,$(LIB_TEST_UTILS_SOURCES),$(LIB_TEST_UTILS_LINK)))


$(eval $(call test,json_diff_test,json_diff vfs value_description arch,boost))
$(eval $(call test,json_hash_test,json_diff value_description arch,boost))
$(eval $(call test,command_expression_test,command_expression value_description arch types test_utils,boost))
$(eval $(call test,config_test,config boost_program_options,boost))
$(eval $(call test,logger_test,log,boost))
$(eval $(call test,compact_vector_test,arch,boost))
$(eval $(call test,fixture_test,test_utils,boost))
$(eval $(call test,print_utils_test,,boost))


$(eval $(call program,runner_test_helper,utils arch io_base value_description))
$(eval $(call test,runner_test,runner arch value_description io_base utils vfs types,boost))
$(eval $(call test,runner_stress_test,runner io_base arch,boost manual))
$(TESTS)/runner_test $(TESTS)/runner_stress_test: $(BIN)/runner_test_helper
$(eval $(call test,sink_test,runner utils io_base arch,boost))

$(eval $(call test,lightweight_hash_test,arch utils,boost))
$(eval $(call test,parse_context_test,utils arch vfs base,boost))

$(eval $(call test,environment_test,utils arch,boost))
$(eval $(call test,string_functions_test,arch utils,boost))
$(eval $(call test,csv_parsing_test,arch utils base,boost))
$(eval $(call test,round_test,,boost))
$(eval $(call test,for_each_line_test,utils log arch block types value_description vfs,boost))
$(eval $(call test,floating_point_test,utils log arch,boost))
$(eval $(call test,safe_clamp_test,utils log arch,catch2))

$(eval $(call test,json_stream_test,utils log arch json_diff types value_description,catch2))
$(eval $(call test,grammar_test,utils log arch base json_diff types value_description,catch2))
