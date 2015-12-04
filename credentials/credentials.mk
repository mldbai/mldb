# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

LIBCREDENTIALSD_SOURCES := \
	credentials_daemon.cc

LIBCREDENTIALSD_LINK := \
	watch rest rest_entity services

$(eval $(call library,credentials_daemon,$(LIBCREDENTIALSD_SOURCES),$(LIBCREDENTIALSD_LINK)))
$(eval $(call program,credentialsd,credentials_daemon boost_program_options))

