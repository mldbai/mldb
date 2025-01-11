# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

PYTHON_INTERPRETER_SOURCES := \
	python_interpreter.cc \
	capture_stream.cc \


PYTHON_INTERPRETER_LINK := \
	$(PYTHON_LIBRARY) \
	vfs \
	arch \
	base \
	value_description \
	types \
	utils \
	runner \
	nanobind \


$(eval $(call set_compile_option,$(PYTHON_INTERPRETER_SOURCES),-I$(PYTHON_INCLUDE_PATH) $(NANOBIND_COMPILE_OPTIONS)))

$(eval $(call library,python_interpreter,$(PYTHON_INTERPRETER_SOURCES),$(PYTHON_INTERPRETER_LINK)))


PYTHON_PLUGIN_SOURCES := \
	python_plugin.cc \
	python_plugin_context.cc \
	python_entities.cc \
	python_converters.cc \


PYTHON_PLUGIN_LINK := \
	value_description \
	$(PYTHON_LIBRARY) \
	mldb_core \
	mldb_builtin_base \
	python_interpreter \
	value_description \
	arch \
	base \
	utils \
	types \
	mldb_engine \
	log \
	any \
	rest \
	rest_entity \
	json_diff \
	logging \
	vfs \
	base \
	sql_types \
	http \
	nanobind \


# Needed so that Python plugin can find its header
$(eval $(call set_compile_option,$(PYTHON_PLUGIN_SOURCES),-I$(PYTHON_INCLUDE_PATH) $(NANOBIND_COMPILE_OPTIONS)))

$(eval $(call library,mldb_python_plugin,$(PYTHON_PLUGIN_SOURCES),$(PYTHON_PLUGIN_LINK)))

$(eval $(call include_sub_make,mldb_python_module,module,module.mk))

$(eval $(call include_sub_make,testing))
