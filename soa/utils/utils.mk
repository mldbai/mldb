# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

#------------------------------------------------------------------------------#
# utils.mk
# Rémi Attab, 26 Jul 2013
# Copyright (c) 2013 Datacratic.  All rights reserved.
#
# Makefile of soa's misc utilities.
#------------------------------------------------------------------------------#

LIB_TEST_UTILS_SOURCES := \
        benchmarks.cc \
        fixtures.cc \
        threaded_test.cc

LIB_TEST_UTILS_LINK := \
	arch utils boost_filesystem

$(eval $(call library,test_utils,$(LIB_TEST_UTILS_SOURCES),$(LIB_TEST_UTILS_LINK)))

$(eval $(call library,csv_writer,csv_writer.cc,utils types))

$(eval $(call include_sub_make,testing))

