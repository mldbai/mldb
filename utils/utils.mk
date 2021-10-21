# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call library,config,config.cc,boost_program_options))
$(eval $(call library,log,log.cc, arch config))


LIBUTILS_SOURCES := \
        environment.cc \
	floating_point.cc \
	buckets.cc \
	confidence_intervals.cc \
	quadtree.cc \
	for_each_line.cc \
	tmpdir.cc \
	fixed_point_accum.cc \
	ansi.cc \

LIBUTILS_LINK := \
	arch \
	vfs \
	db \
	block \
	log \
	arch \
	base \
	types \
	$(STD_FILESYSTEM_LIBNAME)

$(eval $(call library,utils,$(LIBUTILS_SOURCES),$(LIBUTILS_LINK)))

# gcc 4.7
$(eval $(call set_compile_option,hash.cc,-fpermissive))
$(eval $(call set_compile_option,confidence_intervals.cc,-O3))

$(eval $(call library,progress,progress.cc,types value_description arch))

# Runner Common

LIBRUNNERCOMMON_SOURCES := \
	runner_common.cc

LIBRUNNERCOMMON_LINK := arch types value_description

$(eval $(call library,runner_common,$(LIBRUNNERCOMMON_SOURCES),$(LIBRUNNERCOMMON_LINK)))
$(eval $(call program,runner_helper,runner_common arch))

# Runner

LIBRUNNER_SOURCES := \
	sink.cc \
	runner.cc

LIBRUNNER_LINK := runner_common io_base value_description logging utils arch types

$(eval $(call set_compile_option,runner.cc,-DBIN=\"$(BIN)\"))
$(eval $(call library,runner,$(LIBRUNNER_SOURCES),$(LIBRUNNER_LINK)))

$(LIB)/librunner$(SO_EXTENSION): $(BIN)/runner_helper

# Command Expression

LIBCOMMAND_SOURCES := \
	command.cc \

LIBCOMMAND_LINK := runner command_expression value_description arch utils json_diff types base any block

$(eval $(call library,command,$(LIBCOMMAND_SOURCES),$(LIBCOMMAND_LINK)))

$(eval $(call include_sub_make,testing))
