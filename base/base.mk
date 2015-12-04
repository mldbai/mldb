# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


LIBBASE_SOURCES := \
        parse_context.cc \

LIBBASE_LINK :=	arch boost_thread

$(eval $(call library,base,$(LIBBASE_SOURCES),$(LIBBASE_LINK)))

#$(eval $(call include_sub_make,base_testing,testing))

