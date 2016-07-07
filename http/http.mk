# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

LIBHTTP_SOURCES := \
	http_exception.cc \
	http_socket_handler.cc \
	http_header.cc \
	http_parsers.cc \
	http_rest_proxy.cc \
	curl_wrapper.cc \


LIBHTTP_LINK := curl io_base arch jsoncpp types boost_system value_description boost_filesystem cityhash watch

$(eval $(call library,http,$(LIBHTTP_SOURCES),$(LIBHTTP_LINK)))

$(eval $(call include_sub_make,http_testing,testing,http_testing.mk))
