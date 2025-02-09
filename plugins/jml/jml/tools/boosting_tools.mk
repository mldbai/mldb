# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

LIBBOOSTING_TOOLS_SOURCES := \
        boosting_tool_common.cc \
	datasets.cc

LIBBOOSTING_TOOLS_LINK :=	utils db arch base boosting base vfs boost_timer

$(eval $(call library,boosting_tools,$(LIBBOOSTING_TOOLS_SOURCES),$(LIBBOOSTING_TOOLS_LINK)))


$(eval $(call program,classifier_training_tool,boosting boosting_tools utils arch base boost_program_options jml_utils $(if $(findstring 1,$(CUDA_ENABLED)), boosting_cuda),,tools))

$(eval $(call program,training_data_tool,boosting boosting_tools utils arch base boost_program_options,,tools))

