# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# services makefile
# Jeremy Barnes, 29 May 2012


LIBOPSTATS_SOURCES := \
	multi_aggregator.cc \
	statsd_connector.cc \
	stat_aggregator.cc \
	process_stats.cc \
	connectfd.cc

LIBOPSTATS_LINK := \
	arch utils types

$(eval $(call library,opstats,$(LIBOPSTATS_SOURCES),$(LIBOPSTATS_LINK)))

