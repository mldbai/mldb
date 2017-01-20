# js.mk
# Jeremy Barnes, 11 May 2010
# Copyright (c) 2010 mldb.ai inc.  All rights reserved.
#
# Support functions for javascript

LIBRECOSET_JSONCPP_SOURCES := \
	json_reader.cpp \
	json_writer.cpp \
	json_value.cpp

LIBRECOSET_JSONCPP_LINK := 

$(eval $(call library,jsoncpp,$(LIBRECOSET_JSONCPP_SOURCES),$(LIBRECOSET_JSONCPP_LINK)))
