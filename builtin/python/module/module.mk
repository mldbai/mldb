# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


MLDB_PYTHON_ADDON_SOURCES := \
	find_mldb_environment.cc

MLDB_PYTHON_ADDON_LINK := \
	$(PYTHON_LIBRARY) \
	$(BOOST_PYTHON_LIBRARY) \
	mldb_python_plugin \
	arch \

$(eval $(call python_addon,_mldb,$(MLDB_PYTHON_ADDON_SOURCES),$(MLDB_PYTHON_ADDON_LINK)))

$(eval $(call set_compile_option,$(PYTHON_PLUGIN_SOURCES),-I$(PYTHON_INCLUDE_PATH)))

$(eval $(call python_module,mldb,$(notdir $(wildcard $(CWD)/*.py)),_mldb))


# Our mldb_runner binary requires the plugin to be present
$(BIN)/mldb_runner:	| $(PYTHON_mldb_DEPS)
