# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

PYTHON_INTERPRETER_SOURCES := \
	python_interpreter.cc \
	capture_stream.cc \


PYTHON_INTERPRETER_LINK := \
	$(PYTHON_LIBRARY) \
	boost_python3 \
	vfs \

$(eval $(call set_compile_option,$(PYTHON_INTERPRETER_SOURCES),-I$(PYTHON_INCLUDE_PATH)))

$(eval $(call library,python_interpreter,$(PYTHON_INTERPRETER_SOURCES),$(PYTHON_INTERPRETER_LINK)))


PYTHON_PLUGIN_SOURCES := \
	python_plugin.cc \
	python_plugin_context.cc \
	python_entities.cc \
	python_converters.cc \


PYTHON_PLUGIN_LINK := \
	value_description \
	$(PYTHON_LIBRARY) \
	boost_python3 \
	mldb_core \
	mldb_builtin_base \
	python_interpreter \


# Needed so that Python plugin can find its header
$(eval $(call set_compile_option,$(PYTHON_PLUGIN_SOURCES),-I$(PYTHON_INCLUDE_PATH)))

$(eval $(call library,mldb_python_plugin,$(PYTHON_PLUGIN_SOURCES),$(PYTHON_PLUGIN_LINK)))

MLDB_PYTHON_ADDON_SOURCES := \
	find_mldb_environment.cc

MLDB_PYTHON_ADDON_LINK := \
	boost_python3 mldb_python_plugin

$(eval $(call python_addon,_mldb,$(MLDB_PYTHON_ADDON_SOURCES),$(MLDB_PYTHON_ADDON_LINK)))

$(eval $(call set_compile_option,$(PYTHON_PLUGIN_SOURCES),-I$(PYTHON_INCLUDE_PATH)))

$(eval $(call python_module,mldb,$(notdir $(wildcard $(CWD)/*.py)),_mldb))


# Our mldb_runner binary requires the plugin to be present
$(BIN)/mldb_runner:	| $(PYTHON_mldb_DEPS)

$(eval $(call include_sub_make,testing))
