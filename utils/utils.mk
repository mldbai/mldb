# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

$(eval $(call library,config,config.cc,boost_program_options boost_locale))
$(eval $(call library,log,log.cc, config))
$(eval $(call library,json_diff,json_diff.cc json_utils.cc,jsoncpp value_description types utils highwayhash))

LIBCOMMAND_EXPRESSION_SOURCES := \
	command.cc \
	command_expression.cc \


LIBCOMMAND_EXPRESSION_LINK := runner value_description arch utils json_diff

$(eval $(call library,command_expression,$(LIBCOMMAND_EXPRESSION_SOURCES),$(LIBCOMMAND_EXPRESSION_LINK)))

$(eval $(call program,json_format,command_expression vfs_handlers boost_program_options))

$(eval $(call include_sub_make,testing))

