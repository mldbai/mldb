# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# Makefile for tsne functionality
# Jeremy Barnes, 16 January 2010
# Copyright (c) 2010 Jeremy Barnes.  All rights reserved.

LIBTSNE_SOURCES := \
        tsne.cc \
	quadtree.cc

LIBTSNE_LINK :=	utils algebra arch stats

$(eval $(call library,tsne,$(LIBTSNE_SOURCES),$(LIBTSNE_LINK)))

$(eval $(call include_sub_make,tsne_testing,testing))
