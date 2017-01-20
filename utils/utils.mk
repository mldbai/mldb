# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call library,config,config.cc,boost_program_options boost_locale))
$(eval $(call library,log,log.cc, config))
$(eval $(call library,progress,progress.cc,))
$(eval $(call library,json_diff,json_diff.cc json_utils.cc,jsoncpp value_description types utils highwayhash))

# Runner Common

LIBRUNNERCOMMON_SOURCES := \
	runner_common.cc

LIBRUNNERCOMMON_LINK :=

$(eval $(call library,runner_common,$(LIBRUNNERCOMMON_SOURCES),$(LIBRUNNERCOMMON_LINK)))
$(eval $(call program,runner_helper,runner_common arch))

# Runner

LIBRUNNER_SOURCES := \
	sink.cc \
	runner.cc

LIBRUNNER_LINK := runner_common io_base value_description logging utils

$(eval $(call set_compile_option,runner.cc,-DBIN=\"$(BIN)\"))
$(eval $(call library,runner,$(LIBRUNNER_SOURCES),$(LIBRUNNER_LINK)))

$(LIB)/librunner.so: $(BIN)/runner_helper

# Command Expression

LIBCOMMAND_EXPRESSION_SOURCES := \
	command.cc \
	command_expression.cc \

LIBCOMMAND_EXPRESSION_LINK := runner value_description arch utils json_diff

$(eval $(call library,command_expression,$(LIBCOMMAND_EXPRESSION_SOURCES),$(LIBCOMMAND_EXPRESSION_LINK)))

$(eval $(call program,json_format,command_expression vfs_handlers boost_program_options))

$(eval $(call include_sub_make,testing))
