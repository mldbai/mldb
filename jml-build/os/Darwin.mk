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
SO_EXTENSION:=.dylib

VIRTUALENV ?= virtualenv
PYTHON ?= $(VIRTUALENV)/bin/python
PIP ?= $(VIRTUALENV)/bin/pip
PYTHON_DEPENDENCIES_PRE_CMD ?= $(PIP) install -U pip==21.2.3
PYFLAKES ?= $(VIRTUALENV)/bin/flake8 --select=F,E9,E101
J2 ?= $(VIRTUALENV)/bin/j2
J2ENV ?= $(J2) -f env
export VIRTUALENV
LIBRT:=

find_subdirectory__=$(foreach dir,$(firstword $(1)),$(if $(wildcard $(dir)),$(wildcard $(dir)),$(call find_subdirectory,$(wordlist 2,10000,$(1)))))
find_subdirectory=$(if $(call find_subdirectory__,$(1)),$(call find_subdirectory__,$(1)),$(error couldn't find subdirectories $(1)))

HOMEBREW_OPT:=$(call find_subdirectory,/opt/homebrew/opt /usr/local/opt)
HOMEBREW_LIB:=$(call find_subdirectory,/opt/homebrew/lib /usr/local/lib)
HOMEBREW_INCLUDE:=$(call find_subdirectory,/opt/homebrew/include /usr/local/include)

ICU_ROOT:=$(HOMEBREW_OPT)/icu4c
ICU_INCLUDE_PATH:=$(ICU_ROOT)/include/
LIB_icui18n_LINKER_OPTIONS:=-L$(ICU_ROOT)/lib -Wl,-rpath,$(ICU_ROOT)/lib
ICONV_LIBRARY:=iconv
VALGRIND:=
VALGRINDFLAGS:=
OPENSSL_ROOT:=$(HOMEBREW_OPT)/openssl
OPENSSL_INCLUDE_FLAGS:=-I $(OPENSSL_ROOT)/include
LIB_ssl_LINKER_OPTIONS += -L $(OPENSSL_ROOT)/lib
LIB_crypto_LINKER_OPTIONS += -L $(OPENSSL_ROOT)/lib
USE_PLATFORM_V8:=1
V8_ROOT:=$(HOMEBREW_OPT)/v8/libexec/
V8_INCLUDE_PATH:=$(HOMEBREW_OPT)/v8/libexec/include
V8_INCLUDE_FLAGS:=-DV8_COMPRESS_POINTERS=1 -DV8_31BIT_SMIS_ON_64BIT_ARCH=1
LIB_v8_LINKER_OPTIONS:=-L$(V8_ROOT)
LIB_v8_DEPS:= 
PYTHON_INCLUDE_PATH=$(call find_subdirectory,\
	/usr/local/Frameworks/Python.framework/Versions/$(PYTHON_VERSION)/include/python$(PYTHON_VERSION) \
	/opt/homebrew/opt/python@$(PYTHON_VERSION)/Frameworks/Python.framework/Versions/$(PYTHON_VERSION)/include/python$(PYTHON_VERSION))
PYTHON_LIB_PATH=$(call find_subdirectory,\
	/usr/local/Frameworks/Python.framework/Versions/$(PYTHON_VERSION)/lib \
	/opt/homebrew/opt/python@$(PYTHON_VERSION)/Frameworks/Python.framework/Versions/$(PYTHON_VERSION)/lib)
#$(warning PYTHON_INCLUDE_PATH=$(PYTHON_INCLUDE_PATH))
LIB_python3.9_LINKER_OPTIONS=-L $(PYTHON_LIB_PATH)
BOOST_PYTHON_LIBRARY:=boost_python39
LOCAL_INCLUDE_DIR+= $(HOMEBREW_INCLUDE)
#CXXFLAGS+= -I$(HOMEBREW_INCLUDE)
CXXLIBRARYFLAGS+=-L$(ICU_ROOT)/lib -L$(HOMEBREW_LIB)
CXXEXEFLAGS+=-L$(ICU_ROOT)/lib -L$(HOMEBREW_LIB)
LZMA_INCLUDE_PATH:=$(HOMEBREW_INCLUDE)
LZ4_INCLUDE_PATH:=$(HOMEBREW_INCLUDE)
ZSTD_INCLUDE_PATH:=

