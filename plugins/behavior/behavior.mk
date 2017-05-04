# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
# Behaviour makefile
# Jeremy Barnes, 29 July 2010

LIBBEHAVIOR_SOURCES := \
	behavior_domain.cc \
	mapped_behavior_domain.cc \
	mutable_behavior_domain.cc \
	merged_behavior_domain.cc \
	mapped_value.cc \
	behavior_svd.cc \
	behavior_manager.cc \
	boolean_expression.cc \
	boolean_expression_parser.cc \
	behavior_types.cc \
	behavior_utils.cc \
	tranches.cc \
	id.cc

LIBBEHAVIOR_LINK := \
	arch utils db gc runner vfs_handlers svdlibc types value_description ml sql_expression

$(eval $(call set_compile_option,$(LIBBEHAVIOR_SOURCES),-Ipro))

$(eval $(call library,behavior,$(LIBBEHAVIOR_SOURCES),$(LIBBEHAVIOR_LINK)))

$(eval $(call include_sub_make,behavior_testing,testing))
