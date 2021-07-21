MD5SUM:=md5
DEFAULT_TOOLCHAIN:=clang
GNU_TIME:=gtime
GNU_INSTALL:=ginstall
write_timing_to=$(GNU_TIME) -v -o $(1) 
STDOUT_FILENAME:=/dev/stdout
DISTRIB_CODENAME:=Darwin
DIST_CODENAME:=$(shell sw_vers -productVersion)
MACHINE_NAME:=$(shell uname -n)
READLINK:=readlink
linker_rpath=

VIRTUALENV ?= virtualenv
PYTHON ?= $(VIRTUALENV)/bin/python
PIP ?= $(VIRTUALENV)/bin/pip
PYTHON_DEPENDENCIES_PRE_CMD ?= $(PIP) install -U pip==18.0
PYFLAKES ?= $(VIRTUALENV)/bin/flake8 --select=F,E9,E101
J2 ?= $(VIRTUALENV)/bin/j2
J2ENV ?= $(J2) -f env
export VIRTUALENV
LIBRT:=

ICU_ROOT:=/usr/local/opt/icu4c/
ICU_INCLUDE_PATH:=$(ICU_ROOT)/include/
LIB_icui18n_LINKER_OPTIONS:=-L$(ICU_ROOT)/lib
VALGRIND:=
VALGRINDFLAGS:=
OPENSSL_INCLUDE_FLAGS:=-I /usr/local/opt/openssl/include
LIB_ssl_LINKER_OPTIONS += -L /usr/local/opt/openssl/lib
LIB_crypto_LINKER_OPTIONS += -L /usr/local/opt/openssl/lib
USE_PLATFORM_V8:=1
V8_ROOT:=/usr/local/opt/v8/libexec/
V8_INCLUDE_PATH:=/usr/local/opt/v8/libexec/include
V8_INCLUDE_FLAGS:=-DV8_COMPRESS_POINTERS=1 -DV8_31BIT_SMIS_ON_64BIT_ARCH=1
LIB_v8_LINKER_OPTIONS:=-L$(V8_ROOT)
LIB_v8_DEPS:= 
PYTHON_INCLUDE_PATH=/usr/local/Frameworks/Python.framework/Versions/$(PYTHON_VERSION)/include/python$(PYTHON_VERSION)
LIB_python3.9_LINKER_OPTIONS=-L /usr/local/Frameworks/Python.framework/Versions/$(PYTHON_VERSION)/lib
BOOST_PYTHON_LIBRARY:=boost_python39

