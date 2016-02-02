# add a mldb unit test case
# $(1) file in the test (.js or .py)
# $(2) plugins that are used in the testcase
# $(3) options for the test
#      - manual: run the test manually
#      - virtualenv: set up the Python virtualenv even for a non-python test

define mldb_unit_test
ifneq ($(PREMAKE),1)

#TEST_$(1)_COMMAND:=$$(BIN)/mldb_runner -h localhost -p '11700-12700' --run-script $(CWD)/$(1)

# Pre-run setup command.  This currently checks for a Python script, and activates
# it if necessary.
TEST_$(1)_SETUP := $$(if $$(findstring .py,$(1))$$(findstring virtualenv,$(3)),. $$(shell readlink -f $(VIRTUALENV))/bin/activate;)

# Command to actually run for the test.  Constructs the call to mldb_runner and the
# command line options to pass to it.
TEST_$(1)_RAW_COMMAND := $$(BIN)/mldb_runner -h localhost -p '11700-12700' $$(foreach plugin,$(2),--plugin-directory file://$(PLUGINS)/$$(plugin)) --run-script $(CWD)/$(1) --mute-final-output

# Command that is run in the shell.  This takes care of printing the right message
# out and capturing the output in the right place.
TEST_$(1)_COMMAND := rm -f $(TESTS)/$(1).{passed,failed} && ((set -o pipefail && $$(TEST_$(1)_SETUP) $$(TEST_$(1)_RAW_COMMAND) >> $(TESTS)/$(1).running 2>&1 && mv $(TESTS)/$(1).running $(TESTS)/$(1).passed) || (mv $(TESTS)/$(1).running $(TESTS)/$(1).failed && echo "                 $(COLOR_RED)$(1) FAILED$(COLOR_RESET)" && cat $(TESTS)/$(1).failed && echo "                       $(COLOR_RED)$(1) FAILED$(COLOR_RESET)" && false))

$(TESTS)/$(1).passed:	$$(BIN)/mldb_runner  $(CWD)/$(1)
	$$(if $(verbose_build),@echo '$$(TEST_$(1)_COMMAND)',@echo "      $(COLOR_VIOLET)[MLDB TEST]$(COLOR_RESET) $(1)")
	@date -u +%s.%N > $(TESTS)/$(1).timing
	@echo "$$(TEST_$(1)_SETUP) $$(TEST_$(1)_RAW_COMMAND)" > $(TESTS)/$(1).running
	@$$(TEST_$(1)_COMMAND)
	@date -u +%s.%N >> $(TESTS)/$(1).timing
	$$(if $(verbose_build),@echo '$$(TEST_$(1)_COMMAND)',@echo "                 $(COLOR_GREEN)$(1) passed $(COLOR_RESET)$(COLOR_DARK_GRAY)[" `cat $(TESTS)/$(1).timing | awk 'FNR == 1 { start = $$$$1; } FNR == 2 { end = $$$$1; } END { printf("%.1fs", 1.0 * end - 1.0 * start) }'` "]$(COLOR_RESET)")

# If ARGS
TEST_$(1)_ARGS := $$(if $$(findstring $(ARGS), $(ARGS)), $(ARGS), )

$(1):	$$(BIN)/mldb_runner  $(CWD)/$(1)  $$(foreach plugin,$(2),mldb_plugin_$$(plugin))
	$$(TEST_$(1)_SETUP) $$(TEST_$(1)_RAW_COMMAND) $$(TEST_$(1)_ARGS)

.PHONY: $(1)
mldb_unit_tests: $(1)

$(if $(findstring manual,$(3)),manual,test $(if $(findstring noauto,$(3)),,autotest) ) $(CURRENT_TEST_TARGETS) $(CWD)_test_all $(4):	$(TESTS)/$(1).passed
endif
endef

.PHONY: mldb_plugins

# Add an MLDB plugin library
#
# Synopsis:
#    $(eval $(call mldb_plugin_library,<pluginName>,<libName>,<sourceFiles>,<libDeps>))
#
# Arguments:
#    $(1): <pluginName>: name of the plugin
#    $(2): <libName>: name of the library within the plugin
#    $(3): <sourceFiles>: source files to include within the library
#    $(4): <libDeps>: dependency libraries to link with

define mldb_plugin_library
$$(eval $$(call library,$(2),$(3),$(4),,,"$(COLOR_VIOLET)[MLDB PLUGIN SO]$(COLOR_RESET)",$(PLUGINS)/$(1)/lib))
mldb_plugins: $(PLUGINS)/$(1)/lib/lib$(2).so
endef

# $(1): name of the library
# $(2): source files to include in the library
# $(3): libraries to link with
# $(4): output name; default lib$(1)
# $(5): output extension; default .so
# $(6): build name; default SO
# $(7): output dir; default $(LIB)

# Variable that tells us which static content to filter out and not include
# as part of the container.  It is passed to the filter-out function of
# make, which expects a '%' as a wildcard

STATIC_CONTENT_FILTER_PATTERNS:=%~ \#% .%

# Helper macro used by mldb_builtin_plugin to help make to handle a
# static content directory
#
# Synopsis:
#    $(eval $(call mldb_builtin_plugin_static_directory_helper,<pluginName>,<dir>))
#
# Arguments:
#   $(1): <pluginName>: name of the plugin
#   $(2): <dir>: directory that the static content lives in (under CWD)
#
# Side effects:
#   MLDB_PLUGIN_$(1)_STATIC_FILES_$(2) will be set with a list of the names of
#   static files for teh plugins
define mldb_builtin_plugin_static_directory_helper

# Lists all files in the given static content directory.  It
# uses the find command to look for files, then filters them out using the
# STATIC_CONTENT_FILTER_PATTERNS variable.
MLDB_PLUGIN_$(1)_STATIC_FILES_$(2) := $$(filter-out $$(STATIC_CONTENT_FILTER_PATTERNS),$$(shell find $(CWD)/$(2)/ -type f | sed "s!$(CWD)/!!"))

# Debug... print them out
#$$(warning MLDB_PLUGIN_$(1)_STATIC_FILES_$(2) = $$(MLDB_PLUGIN_$(1)_STATIC_FILES_$(2)))

# Tell make that in order to create a file in a plugin static content
# directory, it just has to copy it from the source
$(PLUGINS)/$(1)/$(2)/%:	$(CWD)/$(2)/%
	@echo "   $(COLOR_VIOLET)[MLDB PLUGIN CP]$(COLOR_RESET)" $(1)/$(2)/$$*
	@install -D $$< $$@

endef

# Add an MLDB plugin to be built and installed as part of MLDB
#
# Synopsis:
#
#    $(eval $(call mldb_builtin_plugin,<pluginName>,<libraryList>,<staticContentDirs>))
# Arguments:
#    $(1): <pluginName>: name of the plugin
#    $(2): <libraryList>: list of libraries that should be copied into the plugin
#    $(3): <staticContentDirs>: list of directories that contain static content
#
# The macro will modify variables as follows:
#    - MLDB_BUILTIN_PLUGIN_FILES will have all installed files for the plugins installed
#
# The macro defines the following targets:
#    mldb_plugin_$(1): phony target that depends upon all files in this plugin
#
# The macro sets the following variables:
#    MLDB_PLUGIN_FILES_$(1): a list of all files installed by the plugin

define mldb_builtin_plugin
ifneq ($(PREMAKE),1)

mldb_plugins: mldb_plugin_$(1)
.PHONY: mldb_plugins mldb_plugin_$(1)

# Be able to create target directories
$(PLUGINS)/$(1) $(PLUGINS)/$(1)/lib:
	@mkdir -p $$@

# For each static content directory, create and use it
$$(foreach dir,$(3),$$(eval $$(call mldb_builtin_plugin_static_directory_helper,$(1),$$(dir))))

# This variable lists all files that this plugin needs to make.  It can be
# used to set up full dependency lists for things that depend on the
# plugin.
MLDB_PLUGIN_FILES_$(1) := \
	$(PLUGINS)/$(1)/mldb_plugin.json \
	$$(foreach lib,$(2),$(PLUGINS)/$(1)/lib/lib$$(lib).so) \
	$$(foreach dir,$(3),$$(addprefix $(PLUGINS)/$(1)/,$$(MLDB_PLUGIN_$(1)_STATIC_FILES_$$(dir))))

#$$(warning MLDB_PLUGIN_FILES_$(1) = $$(MLDB_PLUGIN_FILES_$(1)))

# Our plugin target depends upon the files in the plugin...
mldb_plugin_$(1): $$(MLDB_PLUGIN_FILES_$(1))

# Order-only prerequisite on the directory
$(PLUGINS)/$(1)/mldb_plugin.json:	$(CWD)/mldb_plugin.json | $(PLUGINS)/$(1)/lib
	@echo "   $(COLOR_VIOLET)[MLDB PLUGIN MF]$(COLOR_RESET)" $(CWD)/mldb_plugin.json
	@install -D $$< $$@

endif
endef
