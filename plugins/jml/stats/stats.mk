# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

LIBSTATS_SOURCES := \
	auc.cc

$(eval $(call add_sources,$(LIBSTATS_SOURCES)))

LIBSTATS_LINK :=	utils arch

$(eval $(call library,stats,$(LIBSTATS_SOURCES),$(LIBSTATS_LINK)))

$(eval $(call include_sub_make,stats_testing,testing))
