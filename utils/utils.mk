# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

# RTB simulator makefile
# Jeremy Barnes, 16 January 2010

$(eval $(call library,json_diff,json_diff.cc json_utils.cc,jsoncpp value_description types siphash))

LIBCOMMAND_EXPRESSION_SOURCES := \
	command.cc \
	command_expression.cc \


LIBCOMMAND_EXPRESSION_LINK := services value_description arch utils json_diff

$(eval $(call library,command_expression,$(LIBCOMMAND_EXPRESSION_SOURCES),$(LIBCOMMAND_EXPRESSION_LINK)))

$(eval $(call program,json_format,command_expression cloud boost_program_options))

$(eval $(call include_sub_make,testing))

