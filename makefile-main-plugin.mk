# Makefile snippet to include from a plugin which has mldb as a submodule

# Empty default suffixes to speed up initialization
.SUFFIXES:

toolchain ?= gcc
PYTHON_ENABLED:=1
DOCUMENTATION_ENABLED:=1
TCMALLOC_ENABLED?=1

DOCKER_REGISTRY:=quay.io/
DOCKER_USER:=mldb/

# Shim for the 14.04 migration
DIST_CODENAME:=$(call exec-shell, lsb_release -sc)

MACHINE_NAME:=$(call exec-shell, uname -n)

V8_LIB:=v8


-include local.mk
VIRTUALENV ?= virtualenv-$(ARCH)-$(OSNAME)-$(PYTHON_VERSION)
PYTHON ?= $(VIRTUALENV)/bin/python
PIP ?= $(VIRTUALENV)/bin/pip
PYTHON_DEPENDENCIES_PRE_CMD ?= $(PIP) install -U pip=18.0
PYFLAKES ?= $(VIRTUALENV)/bin/flake8 --select=F,E9,E101
J2 ?= $(VIRTUALENV)/bin/j2
J2ENV ?= $(J2) -f env

export VIRTUALENV

default: all
.PHONY: default

BUILD   ?= build
ARCH    ?= $(call exec-shell, uname -m)
OBJ     := $(BUILD)/$(ARCH)/obj
BIN     := $(BUILD)/$(ARCH)/bin
LIB	:= $(BUILD)/$(ARCH)/lib
TESTS   := $(BUILD)/$(ARCH)/tests
TMPBIN	:= $(BUILD)/$(ARCH)/tmp
INC     := $(BUILD)/$(ARCH)/include
SRC     := .
TMP     ?= $(BUILD)/$(ARCH)/tmp

# These are for cross-compilation, where binaries used in the build need
# be be built for the host.
HOSTARCH ?= $(ARCH)
HOSTBIN ?= $(BUILD)/$(HOSTARCH)/bin
HOSTLIB ?= $(BUILD)/$(HOSTARCH)/lib
HOSTINC ?= $(BUILD)/$(HOSTARCH)/include

TEST_TMP := $(TESTS)
# Vars for configuration files or files that live outside bin and lib
ALTROOT := $(BUILD)/$(ARCH)/altroot
ETC     := $(ALTROOT)/etc
PLUGINS := $(BUILD)/$(ARCH)/mldb_plugins

JML_BUILD := mldb/jml-build
INCLUDE := -I. -Imldb

export BIN
export BUILD
export TEST_TMP
export TMP

$(if $(wildcard $(JML_BUILD)/$(toolchain).mk),,$(error toolchain $(toolchain) is unknown.  Currently 'gcc' and 'clang' are supported.))

include $(JML_BUILD)/arch/$(ARCH).mk
include $(JML_BUILD)/$(toolchain).mk

VALGRIND ?= valgrind
VALGRINDFLAGS := --suppressions=mldb/valgrind.supp --error-exitcode=1 --leak-check=full --soname-synonyms=somalloc=*tcmalloc*

include $(JML_BUILD)/functions.mk
include $(JML_BUILD)/rules.mk
include $(JML_BUILD)/python.mk
include $(JML_BUILD)/tcmalloc.mk
include $(JML_BUILD)/docker.mk
include mldb/mldb_macros.mk
#include mldb/release.mk

PREMAKE := 1

$(eval $(call include_sub_makes,mldb $(MLDB_EXTERNAL_PLUGINS)))

PREMAKE := 0

$(eval $(call include_sub_makes,mldb $(MLDB_EXTERNAL_PLUGINS)))
