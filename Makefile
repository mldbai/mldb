default: all
.PHONY: default

exec-shell=$(if $(TRACE_SHELL_COMMANDS),$(warning executing shell command $(1)))$(shell $(1))

JML_BUILD := mldb/jml-build
BUILD   ?= build
ifndef HOSTARCH
HOSTARCH:=$(shell uname -m)
endif
include mldb/jml-build/os/os.mk

toolchain ?= $(DEFAULT_TOOLCHAIN)
port ?= host
PYTHON_ENABLED:=1
DOCUMENTATION_ENABLED:=1
TCMALLOC_ENABLED?=0

DOCKER_REGISTRY:=quay.io/
DOCKER_USER:=mldb/

V8_LIB:=v8


-include local.mk

export VIRTUALENV


# Define our port
include ports.mk

PWD     := $(shell pwd)
ifndef ARCH
ARCH    := $(HOSTARCH)
endif
OBJ     := $(BUILD)/$(ARCH)-$(OSNAME)/obj
BIN     := $(BUILD)/$(ARCH)-$(OSNAME)/bin
LIB		:= $(BUILD)/$(ARCH)-$(OSNAME)/lib
TESTS   := $(BUILD)/$(ARCH)-$(OSNAME)/tests
TMPBIN	:= $(BUILD)/$(ARCH)-$(OSNAME)/tmp
INC     := $(BUILD)/$(ARCH)-$(OSNAME)/include
SRC     := .
TMP     ?= $(PWD)/$(BUILD)/$(ARCH)-$(OSNAME)/tmp

# These are for cross-compilation, where binaries used in the build need
# be be built for the host.
HOSTARCH ?= $(ARCH)
HOSTOSNAME ?= $(OSNAME)
HOSTBIN ?= $(BUILD)/$(HOSTARCH)-$(HOSTOSNAME)/bin
HOSTLIB ?= $(BUILD)/$(HOSTARCH)-$(HOSTOSNAME)/lib
HOSTINC ?= $(BUILD)/$(HOSTARCH)-$(HOSTOSNAME)/include

TEST_TMP := $(TESTS)
# Vars for configuration files or files that live outside bin and lib
ALTROOT := $(BUILD)/$(ARCH)/altroot
ETC     := $(ALTROOT)/etc
PLUGINS := $(BUILD)/$(ARCH)-$(OSNAME)/mldb_plugins

INCLUDE := -Imldb -Imldb/ext/include

export BIN
export BUILD
export TEST_TMP
export TMP
export LIB
export SO_EXTENSION

$(if $(wildcard $(JML_BUILD)/$(toolchain).mk),,$(error toolchain $(toolchain) is unknown.  Currently 'gcc', 'gcc6', 'gcc7', 'gcc8' and 'clang' are supported [looking for $(JML_BUILD)/$(toolchain).mk].))

include $(JML_BUILD)/arch/$(ARCH).mk
include $(JML_BUILD)/$(toolchain).mk

VALGRIND ?= valgrind
VALGRINDFLAGS ?= --soname-synonyms=somalloc=*tcmalloc* --suppressions=valgrind.supp --error-exitcode=1 --leak-check=full

include $(JML_BUILD)/port.mk
include $(JML_BUILD)/functions.mk
include $(JML_BUILD)/rules.mk
include $(JML_BUILD)/python.mk
include $(JML_BUILD)/tcmalloc.mk
include $(JML_BUILD)/docker.mk
include mldb/mldb_macros.mk
include mldb/release.mk

PREMAKE := 1

$(eval $(call include_sub_make,mldb))

PREMAKE := 0

$(eval $(call include_sub_make,mldb))


