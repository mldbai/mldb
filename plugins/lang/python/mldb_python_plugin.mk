# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.



PYTHON_PLUGIN_SOURCES := \
	python_loader.cc \
	python_plugin_context.cc \
	python_entities.cc \
	python_converters.cc

# Needed so that Python plugin can find its header
$(eval $(call set_compile_option,$(PYTHON_PLUGIN_SOURCES),-I$(PYTHON_INCLUDE_PATH)))

$(eval $(call library,mldb_python_plugin,$(PYTHON_PLUGIN_SOURCES),value_description python2.7 boost_python mldb_core))

$(eval $(call include_sub_make,testing))
