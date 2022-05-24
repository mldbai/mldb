# re2.mk
# Jeremy Barnes, 16 December 2015
# Copyright (c) 2015 mldb.ai inc.  All rights reserved.
#
# Build instructions for Google's re2 regular expression engine

RE2_FILES:=$(shell find $(CWD) -name "*.cc" | grep -v test | grep -v 'threadwin\|fuzz\|benchmark')
RE2_SOURCES:=$(RE2_FILES:$(CWD)/%=%)

ifeq ($(toolchain),gcc6)
RE2_WARNING_FLAGS := -Wno-misleading-indentation
endif

ifeq ($(toolchain),gcc7)
RE2_WARNING_FLAGS := -Wno-misleading-indentation
endif

ifeq ($(toolchain),gcc8)
RE2_WARNING_FLAGS := -Wno-misleading-indentation -Wno-parentheses -Wno-class-memaccess
endif

ifeq ($(toolchain),gcc9)
RE2_WARNING_FLAGS := -Wno-misleading-indentation -Wno-parentheses -Wno-class-memaccess
endif

ifeq ($(toolchain),gcc10)
RE2_WARNING_FLAGS := -Wno-misleading-indentation -Wno-parentheses -Wno-class-memaccess
endif

ifeq ($(toolchain),clang)
RE2_WARNING_FLAGS := -Wno-misleading-indentation -Wno-parentheses -Wno-unused-but-set-variable
endif

$(eval $(call set_compile_option,$(RE2_SOURCES),-I$(CWD) $(RE2_WARNING_FLAGS)))

$(eval $(call library,re2,$(RE2_SOURCES)))

RE2_INCLUDE_PATH := mldb/ext/re2

