toolchain ?= gcc
port ?= host
PYTHON_ENABLED:=1
DOCUMENTATION_ENABLED:=1
TCMALLOC_ENABLED?=1

DOCKER_REGISTRY:=quay.io/
DOCKER_USER:=mldb/

# Shim for the 14.04 migration
DIST_CODENAME:=$(shell lsb_release -sc)

MACHINE_NAME:=$(shell uname -n)

V8_LIB:=v8
HOSTARCH    ?= $(shell uname -m)


-include local.mk
VIRTUALENV ?= virtualenv
PYTHON ?= $(VIRTUALENV)/bin/python
PIP ?= $(VIRTUALENV)/bin/pip
PYTHON_DEPENDENCIES_PRE_CMD ?= $(PIP) install -U pip==8.0.2
PYFLAKES ?= $(VIRTUALENV)/bin/flake8 --select=F,E9,E101
J2 ?= $(VIRTUALENV)/bin/j2
J2ENV ?= $(J2) -f env

export VIRTUALENV

default: all
.PHONY: default

# Define our port
include ports.mk

PWD     := $(shell pwd)
BUILD   ?= build
ARCH    ?= $(HOSTARCH)
OBJ     := $(BUILD)/$(ARCH)/obj
BIN     := $(BUILD)/$(ARCH)/bin
LIB	:= $(BUILD)/$(ARCH)/lib
TESTS   := $(BUILD)/$(ARCH)/tests
TMPBIN	:= $(BUILD)/$(ARCH)/tmp
INC     := $(BUILD)/$(ARCH)/include
SRC     := .
TMP     ?= $(PWD)/$(BUILD)/$(ARCH)/tmp

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
INCLUDE := -Imldb

export BIN
export BUILD
export TEST_TMP
export TMP

$(if $(wildcard $(JML_BUILD)/$(toolchain).mk),,$(error toolchain $(toolchain) is unknown.  Currently 'gcc', 'gcc5', 'gcc6' and 'clang' are supported.))

include $(JML_BUILD)/arch/$(ARCH).mk
include $(JML_BUILD)/$(toolchain).mk

VALGRIND ?= valgrind
VALGRINDFLAGS := --soname-synonyms=somalloc=*tcmalloc* --suppressions=valgrind.supp --error-exitcode=1 --leak-check=full

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


