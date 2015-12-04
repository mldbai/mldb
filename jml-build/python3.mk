ifeq ($(PYTHON3_ENABLED),1)

PYTHON3_VERSION_DETECTED := $(shell $(JML_BUILD)/detect_python3.sh)
PYTHON3_VERSION ?= $(PYTHON3_VERSION_DETECTED)

PYTHON3_INCLUDE_PATH ?= $(VIRTUALENV3)/include/python3$(PYTHON3_VERSION)
PYTHON3 ?= python3$(PYTHON3_VERSION)
PIP3 ?= pip
PYFLAKES3 ?= true
# Override this to run a cmd before installing python3_requirements.txt
PYTHON3_DEPENDENCIES_PRE_CMD ?= true

PYTHON3_PURE_LIB_PATH ?= $(BIN)
PYTHON3_PLAT_LIB_PATH ?= $(BIN)
PYTHON3_BIN_PATH ?= $(BIN)

RUN_PYTHONPATH := $(if $(PYTHONPATH),$(PYTHONPATH):,)$(PYTHON3_PURE_LIB_PATH):$(PYTHON3_PLAT_LIB_PATH):$(PYTHON3_BIN_PATH)

PYTHONPATH ?= RUN_PYTHONPATH

ifdef VIRTUALENV3

$(VIRTUALENV3)/bin/activate:
	virtualenv -p python3 $(VIRTUALENV3)

python3_dependencies: $(VIRTUALENV3)/bin/activate

PYTHON3_EXECUTABLE ?= $(PYTHON3) $(PYTHON3_ARGS)

endif

python3_dependencies:
	if [ -f python3_requirements.txt ]; then \
		$(PYTHON3_DEPENDENCIES_PRE_CMD); \
		$(PIP3) install -r python3_requirements.txt; \
	fi

# Loop over the python3_extra_requirements.txt file and install packages in
# order. We did that because the package "statsmodels" does not handle
# dependencies that are not installed. For more information, see:
# https://github.com/statsmodels/statsmodels/pull/1902
# https://github.com/statsmodels/statsmodels/issues/1897
#
dependencies: python3_dependencies

# add a swig wrapper source file
# $(1): filename of source file
# $(2): basename of the filename
define add_swig_source
ifneq ($(PREMAKE),1)
$(if $(trace),$$(warning called add_swig_source "$(1)" "$(2)"))

BUILD_$(OBJ)/$(CWD)/$(2)_wrap.cxx_COMMAND := swig -python3 -c++  -MMD -MF $(OBJ)/$(CWD)/$(2).d -MT "$(OBJ)/$(CWD)/$(2)_wrap.cxx $(OBJ)/$(CWD)/$(2).lo" -o $(OBJ)/$(CWD)/$(2)_wrap.cxx~ $(SRC)/$(CWD)/$(1)

# Call swig to generate the source file
$(OBJ)/$(CWD)/$(2)_wrap.cxx:	$(SRC)/$(CWD)/$(1)
	@mkdir -p $(OBJ)/$(CWD)
	$$(if $(verbose_build),@echo $$(BUILD_$(OBJ)/$(CWD)/$(2)_wrap.cxx_COMMAND),@echo "[SWIG python3] $(CWD)/$(1)")
	@$$(BUILD_$(OBJ)/$(CWD)/$(2)_wrap.cxx_COMMAND)
	@mv $$@~ $$@

# We use the add_c++_source to do most of the work, then simply point
# to the file
$$(eval $$(call add_c++_source,$(2)_wrap.cxx,$(2)_wrap,$(OBJ),-I$(PYTHON3_INCLUDE_PATH)))

# Point to the object file produced by the previous macro
BUILD_$(CWD)/$(2).lo_OBJ  := $$(BUILD_$(CWD)/$(2)_wrap.lo_OBJ)

-include $(OBJ)/$(CWD)/$(2).d

endif
endef

# python3 test case

# $(1) name of the test
# $(2) python3 modules on which it depends
# $(3) test options (e.g. manual)
# $(4) test targets

define python3_test
ifneq ($(PREMAKE),1)
$$(if $(trace),$$(warning called python3_test "$(1)" "$(2)" "$(3)" "$(4)"))

TEST_$(1)_COMMAND := rm -f $(TESTS)/$(1).{passed,failed} && $(PYFLAKES3) $(CWD)/$(1).py && ((set -o pipefail && PYTHONPATH=$(RUN_PYTHONPATH) $(PYTHON3) $(PYTHON3_ARGS) $(CWD)/$(1).py > $(TESTS)/$(1).running 2>&1 && mv $(TESTS)/$(1).running $(TESTS)/$(1).passed) || (mv $(TESTS)/$(1).running $(TESTS)/$(1).failed && echo "                 $(COLOR_RED)$(1) FAILED$(COLOR_RESET)" && cat $(TESTS)/$(1).failed && false))

$(TESTS)/$(1).passed:	$(TESTS)/.dir_exists $(CWD)/$(1).py $$(foreach lib,$(2),$$(PYTHON3_$$(lib)_DEPS)) $$(foreach pymod,$(2),$(TMPBIN)/$$(pymod)_pymod)
	$$(if $(verbose_build),@echo '$$(TEST_$(1)_COMMAND)',@echo "      $(COLOR_VIOLET)[TESTCASE]$(COLOR_RESET) $(1)")
	@$$(TEST_$(1)_COMMAND)
	$$(if $(verbose_build),@echo '$$(TEST_$(1)_COMMAND)',@echo "                 $(COLOR_GREEN)$(1) passed$(COLOR_RESET)")

$(1):	$(CWD)/$(1).py $$(foreach lib,$(2),$$(PYTHON3_$$(lib)_DEPS)) $$(foreach pymod,$(2),$(TMPBIN)/$$(pymod)_pymod)
	@$(PYFLAKES3) $(CWD)/$(1).py
	PYTHONPATH=$(RUN_PYTHONPATH) $(PYTHON3) $(PYTHON3_ARGS) $(CWD)/$(1).py $($(1)_ARGS)

.PHONY: $(1)

$(if $(findstring manual,$(3)),manual,test $(if $(findstring noauto,$(3)),,autotest) ) $(CURRENT_TEST_TARGETS) $$(CURRENT)_test $(4) python3_test:	$(TESTS)/$(1).passed
endif
endef

# $(1): name of python3 file
# $(2): name of directory to go in

define install_python3_file
ifneq ($(PREMAKE),1)

$$(if $(trace),$$(warning called install_python3_file "$(1)" "$(2)"))

$(PYTHON3_PURE_LIB_PATH)/$(2)/$(1):	$(CWD)/$(1) $(PYTHON3_PURE_LIB_PATH)/$(2)/.dir_exists
	$$(if $(verbose_build),@echo "cp $$< $$@",@echo " $(COLOR_YELLOW)[PYTHON3_MODULE]$(COLOR_RESET) $(2)/$(1)")
	@$(PYFLAKES3) $$<
	@cp $$< $$@~
	@mv $$@~ $$@

#$$(w arning building $(BIN)/$(2)/$(1))

all compile: $(PYTHON3_PURE_LIB_PATH)/$(2)/$(1)

endif
endef

# $(1): name of python3 module
# $(2): list of python3 source files to copy
# $(3): python3 modules it depends upon
# $(4): libraries it depends upon

define python3_module
ifneq ($(PREMAKE),1)
$$(if $(trace),$$(warning called python3_module "$(1)" "$(2)" "$(3)" "$(4)"))

$$(foreach file,$(2),$$(eval $$(call install_python3_file,$$(file),$(1))))

PYTHON3_$(1)_DEPS := $$(foreach file,$(2),$(PYTHON3_PURE_LIB_PATH)/$(1)/$$(file)) $$(foreach pymod,$(3),$(TMPBIN)/$$(pymod)_pymod) $$(foreach pymod,$(3),$$(PYTHON3_$$(pymod)_DEPS)) $$(foreach lib,$(4),$$(LIB_$$(lib)_DEPS))

#$$(w arning PYTHON3_$(1)_DEPS=$$(PYTHON3_$(1)_DEPS))

$(TMPBIN)/$(1)_pymod: $$(PYTHON3_$(1)_DEPS)
	@mkdir -p $$(dir $$@)
	@touch $(TMPBIN)/$(1)_pymod

python3_modules: $$(PYTHON3_$(1)_DEPS) $(TMPBIN)/$(1)_pymod

all compile:	python3_modules
endif
endef

# $(1): name of python3 program
# $(2): python3 source file to copy
# $(3): python3 modules it depends upon

define python3_program
ifneq ($(PREMAKE),1)
$$(if $(trace),$$(warning called python3_program "$(1)" "$(2)" "$(3)"))

PYTHON3_$(1)_DEPS := $(PYTHON3_BIN_PATH)/$(1) $$(foreach pymod,$(3),$$(PYTHON3_$$(pymod)_DEPS))

.PHONY: run_$(1)

run_$(1):	$(PYTHON3_BIN_PATH)/$(1)
	$(PYTHON3) $(PYTHON3_ARGS) $(PYTHON3_BIN_PATH)/$(1)  $($(1)_ARGS)

$(PYTHON3_BIN_PATH)/$(1): $(CWD)/$(2) $(PYTHON3_BIN_PATH)/.dir_exists $$(foreach pymod,$(3),$(TMPBIN)/$$(pymod)_pymod) $$(foreach pymod,$(3),$$(PYTHON3_$$(pymod)_DEPS))
	@echo "$(COLOR_BLUE)[PYTHON3_PROGRAM]$(COLOR_RESET) $(1)"
	@$(PYFLAKES3) $$<
	@(echo "#!$(PYTHON3_EXECUTABLE)"; cat $$<) > $$@~
	@chmod +x $$@~
	@mv $$@~ $$@

#$$(w arning PYTHON3_$(1)_DEPS=$$(PYTHON3_$(1)_DEPS))

$(1): $(PYTHON3_BIN_PATH)/$(1)

python3_programs: $$(PYTHON3_$(1)_DEPS)

all compile:	python3_programs
endif
endef

# add a python3 addon
# $(1): name of the addon
# $(2): source files to include in the addon
# $(3): libraries to link with

define python3_addon
$$(eval $$(call set_compile_option,$(2),-I$$(PYTHON3_INCLUDE_PATH)))
$$(eval $$(call library,$(1),$(2),$(3) boost_python3,$(1),,"  $(COLOR_YELLOW)[PYTHON3_ADDON]$(COLOR_RESET)"))

ifneq ($(PREMAKE),1)

ifneq ($(LIB),$(PYTHON3_PLAT_LIB_PATH))
$(PYTHON3_PLAT_LIB_PATH)/$(1).so:	$(LIB)/$(1).so
	@cp $$< $$@~ && mv $$@~ $$@
endif


$(TMPBIN)/$(1)_pymod: $(PYTHON3_PLAT_LIB_PATH)/$(1).so
	@mkdir -p $$(dir $$@)
	@touch $(TMPBIN)/$(1)_pymod

python3_modules: $(PYTHON3_PLAT_LIB_PATH)/$(1).so

endif
endef

# adds a C++ program as a dependency of a python3 test script
# $(1): name of the python3 test script
# $(2): list of C++ dependencies

define add_python3_test_dep
ifneq ($(PREMAKE),1)

$(CWD)/$(1).py: $$(foreach dep,$(2),$(BIN)/$$(dep))

endif
endef

endif # ifeq ($(PYTHON3_ENABLED),1)
