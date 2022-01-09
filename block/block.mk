# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# TODO HACK
LIBCOMMAND_EXPRESSION_SOURCES := \
	../utils/command_expression.cc \

LIBCOMMAND_EXPRESSION_LINK := value_description arch types base any

$(eval $(call library,command_expression,$(LIBCOMMAND_EXPRESSION_SOURCES),$(LIBCOMMAND_EXPRESSION_LINK)))

#$(eval $(call program,json_format,command_expression boost_program_options value_description base vfs))


LIBBLOCK_SOURCES:= \
	memory_region.cc \
	zip_serializer.cc \
	file_serializer.cc \
	content_descriptor.cc \
	content.cc \
	compute_kernel.cc \
	compute_kernel_host.cc \
	compute_kernel_multi.cc \
	compute_kernel_grid.cc \

$(eval $(call library,block,$(LIBBLOCK_SOURCES),vfs $(LIBARCHIVE_LIB_NAME) types arch db base value_description any command_expression))

$(eval $(call include_sub_make,testing))

