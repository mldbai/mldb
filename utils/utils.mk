# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call library,config,config.cc,boost_program_options arch))
$(eval $(call library,log,log.cc, arch config))


LIBUTILS_SOURCES := \
    environment.cc \
    string_functions.cc \
	csv.cc \
	floating_point.cc \
	buckets.cc \
	confidence_intervals.cc \
	quadtree.cc \
	for_each_line.cc \
	block_splitter.cc \
	tmpdir.cc \

LIBUTILS_LINK := \
	arch \
	vfs \
	db \
	block \
	log \
	arch \
	base \
	types \
	value_description \
	$(STD_FILESYSTEM_LIBNAME) \

$(eval $(call library,utils,$(LIBUTILS_SOURCES),$(LIBUTILS_LINK)))

# gcc 4.7
$(eval $(call set_compile_option,hash.cc,-fpermissive))
$(eval $(call set_compile_option,confidence_intervals.cc,-O3))

$(eval $(call library,progress,progress.cc,types value_description arch base types))
$(eval $(call library,lisp,lisp.cc lisp_value.cc lisp_parsing.cc lisp_lib.cc lisp_predicate.cc,arch base value_description types utils highwayhash any))
$(eval $(call library,json_diff,json_diff.cc json_utils.cc json_stream.cc grammar.cc,arch base value_description types utils highwayhash any))

# Runner Common

LIBRUNNERCOMMON_SOURCES := \
	runner_common.cc

LIBRUNNERCOMMON_LINK := arch base types value_description

$(eval $(call library,runner_common,$(LIBRUNNERCOMMON_SOURCES),$(LIBRUNNERCOMMON_LINK)))
$(eval $(call program,runner_helper,runner_common arch))

# Runner

LIBRUNNER_SOURCES := \
	sink.cc \
	runner.cc

LIBRUNNER_LINK := runner_common io_base value_description logging utils arch base types

$(eval $(call set_compile_option,runner.cc,-DBIN=\"$(BIN)\"))
$(eval $(call library,runner,$(LIBRUNNER_SOURCES),$(LIBRUNNER_LINK)))

$(LIB)/librunner$(SO_EXTENSION): $(BIN)/runner_helper

# Command Expression

LIBCOMMAND_EXPRESSION_SOURCES := \
	command.cc \
	command_expression.cc \

LIBCOMMAND_EXPRESSION_LINK := runner value_description arch utils json_diff types base any

$(eval $(call library,command_expression,$(LIBCOMMAND_EXPRESSION_SOURCES),$(LIBCOMMAND_EXPRESSION_LINK)))

$(eval $(call program,json_format,command_expression boost_program_options value_description base vfs))

$(eval $(call include_sub_make,testing))
