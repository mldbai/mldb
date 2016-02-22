# re2.mk
# Jeremy Barnes, 16 December 2015
# Copyright (c) 2015 Datacratic Inc.  All rights reserved.
#
# Build instructions for Google's re2 regular expression engine

RE2_FILES:=$(shell find $(CWD) -name "*.cc" | grep -v test | grep -v threadwin)
RE2_SOURCES:=$(RE2_FILES:$(CWD)/%=%)

$(eval $(call set_compile_option,$(RE2_SOURCES),-I$(CWD)))

$(eval $(call library,re2,$(RE2_SOURCES)))


