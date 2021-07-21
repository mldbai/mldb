MD5SUM:=md5sum
DEFAULT_TOOLCHAIN:=gcc
GNU_TIME:=/usr/bin/time
GNU_INSTALL=install
STDOUT_FILENAME:=/dev/fd/1
write_timing_to=$(GNU_TIME) -v -o $(1)
linker_rpath=-Wl,--rpath,$(1)

# Include the lsb-release file to know what version of v8 to link with
include /etc/lsb-release

DIST_CODENAME:=$(shell lsb_release -sc)
MACHINE_NAME:=$(shell uname -n)
READLINK:=readlink -f

VIRTUALENV ?= virtualenv
PYTHON ?= $(VIRTUALENV)/bin/python
PIP ?= $(VIRTUALENV)/bin/pip
PYTHON_DEPENDENCIES_PRE_CMD ?= $(PIP) install -U pip==21.1.3
PYFLAKES ?= $(VIRTUALENV)/bin/flake8 --select=F,E9,E101
J2 ?= $(VIRTUALENV)/bin/j2
J2ENV ?= $(J2) -f env
LIBRT:=rt

export VIRTUALENV
