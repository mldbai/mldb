MD5SUM:=md5sum
DEFAULT_TOOLCHAIN:=clang
GNU_TIME:=/usr/bin/time
GNU_INSTALL=install
STDOUT_FILENAME:=/dev/fd/1
write_timing_to=$(GNU_TIME) -v -o $(1)
linker_rpath=-Wl,--rpath,$(1)

SO_EXTENSION:=.so

# Include the lsb-release file to know what version of v8 to link with
include /etc/lsb-release

DIST_CODENAME:=$(call exec-shell, lsb_release -sc)
MACHINE_NAME:=$(call exec-shell, uname -n)
READLINK:=readlink -f

VIRTUALENV ?= virtualenv-$(ARCH)-$(OSNAME)-$(PYTHON_VERSION)
SYSTEM_PYTHON ?= python$(PYTHON_VERSION)
PYTHON ?= $(VIRTUALENV)/bin/python
PIP ?= $(VIRTUALENV)/bin/pip
PYTHON_DEPENDENCIES_PRE_CMD ?= $(PIP) install -U pip==24.2
PYFLAKES ?= true # $(VIRTUALENV)/bin/flake8 --select=F,E9,E101
J2 ?= $(VIRTUALENV)/bin/j2
J2ENV ?= $(J2) -f env
LIBRT:=rt

ICU_ROOT?=/usr
ICU_INCLUDE_PATH?=$(ICU_ROOT)/include/
ICONV_LIBRARY:=

export VIRTUALENV
