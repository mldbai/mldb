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

VIRTUALENV ?= virtualenv-$(ARCH)-$(OSNAME)-$(PYTHON_VERSION)
SYSTEM_PYTHON ?= python$(PYTHON_VERSION)
PIP ?= pip$(PYTHON_VERSION)
PYTHON_DEPENDENCIES_PRE_CMD ?= $(PIP) install -U pip==24.2
PYFLAKES ?= true # $(VIRTUALENV)/bin/flake8 --select=F,E9,E101
J2 ?= $(VIRTUALENV)/bin/j2
J2ENV ?= $(J2) -f env
PYTHON_VERSION:=3.12
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
V8_INCLUDE_FLAGS:=-DV8_COMPRESS_POINTERS=1 -DV8_31BIT_SMIS_ON_64BIT_ARCH=1 -DV8_ENABLE_SANDBOX
LIB_v8_LINKER_OPTIONS:=-L$(V8_ROOT)
LIB_v8_DEPS:= 
PYTHON_VERSION_DETECTED:=$(if $(PYTHON_VERSION_DETECTED),$(PYTHON_VERSION_DETECTED),$(shell $(JML_BUILD)/detect_python.sh))
PYTHON_VERSION?=$(PYTHON_VERSION_DETECTED)
#$(warning PYTHON_VERSION=$(PYTHON_VERSION))
PYTHON_INCLUDE_PATH:=$(call find_subdirectory,\
	/usr/local/Frameworks/Python.framework/Versions/$(PYTHON_VERSION)/include/python$(PYTHON_VERSION) \
	/opt/homebrew/opt/python@$(PYTHON_VERSION)/Frameworks/Python.framework/Versions/$(PYTHON_VERSION)/include/python$(PYTHON_VERSION))
PYTHON_LIB_PATH:=$(call find_subdirectory,\
	/usr/local/Frameworks/Python.framework/Versions/$(PYTHON_VERSION)/lib \
	/opt/homebrew/opt/python@$(PYTHON_VERSION)/Frameworks/Python.framework/Versions/$(PYTHON_VERSION)/lib)
#$(warning PYTHON_INCLUDE_PATH=$(PYTHON_INCLUDE_PATH))
#$(warning PYTHON_LIB_PATH=$(PYTHON_LIB_PATH))
LIB_python$(PYTHON_VERSION)_LINKER_OPTIONS=-L $(PYTHON_LIB_PATH)
BOOST_PYTHON_LIBRARY:=boost_python312
LOCAL_INCLUDE_DIR+= $(HOMEBREW_INCLUDE)
#CXXFLAGS+= -I$(HOMEBREW_INCLUDE)
CXXLIBRARYFLAGS+=-L$(ICU_ROOT)/lib -L$(HOMEBREW_LIB)
CXXEXEFLAGS+=-L$(ICU_ROOT)/lib -L$(HOMEBREW_LIB)
LZMA_INCLUDE_PATH:=$(HOMEBREW_INCLUDE)
LZ4_INCLUDE_PATH:=$(HOMEBREW_INCLUDE)
ZSTD_INCLUDE_PATH:=
LLVM_INCLUDE_PATH:=$(HOMEBREW_OPT)/llvm/include
LIB_llvm_LINKER_OPTIONS:=-L$(HOMEBREW_OPT)/llvm/lib
LIB_OpenCL_LINKER_OPTIONS:=-framework OpenCL
LIB_OpenCL_HAS_NO_SHLIB:=1
LIB_Metal_LINKER_OPTIONS:=-framework Metal
LIB_Metal_HAS_NO_SHLIB:=1
LIB_MetalKit_LINKER_OPTIONS:=-framework MetalKit
LIB_MetalKit_HAS_NO_SHLIB:=1
LIB_Cocoa_LINKER_OPTIONS:=-framework Cocoa
LIB_Cocoa_HAS_NO_SHLIB:=1
LIB_CoreFoundation_LINKER_OPTIONS:=-framework CoreFoundation
LIB_CoreFoundation_HAS_NO_SHLIB:=1

PYTHON_VERSION=3.12

# This provides attributes allowing easier debugability including core files
POST_LINK_COMMAND:=codesign -s - -f --entitlements $(JML_BUILD)/os/mldb.debug.entitlements.plist
