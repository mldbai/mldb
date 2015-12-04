CC := gcc
CXX := g++
PYTHON_ENABLED:=1
DOCUMENTATION_ENABLED:=1
TCMALLOC_ENABLED?=1

DOCKER_REGISTRY:=quay.io/
DOCKER_USER:=datacratic/

LOCAL_DIR?=$(HOME)/local
LOCAL_LIB_DIR?=$(LOCAL_DIR)/lib /usr/local/lib
LOCAL_INCLUDE_DIR?=$(LOCAL_DIR)/include

# Shim for the 14.04 migration
DIST_CODENAME:=$(shell lsb_release -sc)

V8_INCLUDE_PATH?=$(LOCAL_INCLUDE_DIR)/v8
MACHINE_NAME:=$(shell uname -n)

V8_LIB:=v8


-include local.mk
VIRTUALENV ?= virtualenv
PYTHON ?= $(VIRTUALENV)/bin/python
PIP ?= $(VIRTUALENV)/bin/pip
PYTHON_DEPENDENCIES_PRE_CMD ?= $(PIP) install -U pip==7.1.0
PYFLAKES ?= $(VIRTUALENV)/bin/flake8 --select=F,E9,E101
J2 ?= $(VIRTUALENV)/bin/j2
J2ENV ?= $(J2) -f env

export VIRTUALENV

default: all
.PHONY: default

BUILD   ?= build
ARCH    ?= $(shell uname -m)
OBJ     := $(BUILD)/$(ARCH)/obj
BIN     := $(BUILD)/$(ARCH)/bin
LIB	:= $(BUILD)/$(ARCH)/lib
TESTS   := $(BUILD)/$(ARCH)/tests
TMPBIN	:= $(BUILD)/$(ARCH)/tmp
INC     := $(BUILD)/$(ARCH)/include
SRC     := .
TMP     ?= $(BUILD)/$(ARCH)/tmp
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

include $(JML_BUILD)/arch/$(ARCH).mk

CXX_VERSION?=$(shell g++ --version | head -n1 | sed 's/.* //g')

CFLAGS += -fno-strict-overflow -msse4.2

CXXFLAGS += -Wno-deprecated -Winit-self -fno-omit-frame-pointer -std=c++0x -fno-deduce-init-list -msse3 -Wno-unused-but-set-variable -I$(LOCAL_INCLUDE_DIR) -I/usr/local/include -Wno-psabi -D__GXX_EXPERIMENTAL_CXX0X__=1 -msse4.2 -I$(V8_INCLUDE_PATH) -DNODEJS_DISABLED=1 -D_GLIBCXX_USE_NANOSLEEP=1 -D_GLIBCXX_USE_SCHED_YIELD=1
CXXLINKFLAGS += -Wl,--copy-dt-needed-entries -Wl,--no-as-needed -L/usr/local/lib
CFLAGS +=  -Wno-unused-but-set-variable

VALGRINDFLAGS := --suppressions=valgrind.supp --error-exitcode=1 --leak-check=full --soname-synonyms=somalloc=*tcmalloc*

$(if $(findstring x4.5,x$(CXX_VERSION)),$(eval CXXFLAGS += -Dnoexcept= -Dnullptr=NULL))
$(if $(findstring x4.8,x$(CXX_VERSION)),$(eval CXXFLAGS += -Wno-unused-local-typedefs -Wno-return-local-addr))
$(if $(findstring x4.9,x$(CXX_VERSION)),$(eval CXXFLAGS += -Wno-unused-local-typedefs))
$(if $(findstring x5.1,x$(CXX_VERSION)),$(eval CXXFLAGS += -Wno-unused-local-typedefs -Wno-unused-variable))

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


