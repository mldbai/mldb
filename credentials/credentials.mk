# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# credentials makefile
# Jeremy Barnes, 5 November 2014
# Copyright (c) 2014 mldb.ai inc.  All rights reserved.

LIBCREDENTIALS_SOURCES := \
	credentials.cc credential_provider.cc

LIBCREDENTIALS_LINK := \
	arch utils types value_description

$(eval $(call library,credentials,$(LIBCREDENTIALS_SOURCES),$(LIBCREDENTIALS_LINK)))
