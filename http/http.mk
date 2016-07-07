# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

LIBSERVICES_BASE_SOURCES := \
	http_header.cc \
	http_exception.cc \
	logs.cc \

LIBSERVICES_BASE_LINK := value_description

$(eval $(call library,services_base,$(LIBSERVICES_BASE_SOURCES),$(LIBSERVICES_BASE_LINK)))

LIBHTTP_SOURCES := \
	asio_thread_pool.cc \
	asio_timer.cc \
	port_range_service.cc \
	event_loop.cc \
	tcp_acceptor.cc \
	tcp_socket_handler.cc \
	tcp_socket.cc \
	http_socket_handler.cc \
	http_parsers.cc \
	http_rest_proxy.cc \
	event_loop_impl.cc \
	tcp_acceptor_impl.cc \
	tcp_socket_handler_impl.cc \
	curl_wrapper.cc \

#	epoller.cc \
#	epoll_loop.cc \
#	message_loop.cc \


LIBHTTP_LINK := curl arch jsoncpp types boost_system value_description boost_filesystem cityhash services_base watch

$(eval $(call library,http,$(LIBHTTP_SOURCES),$(LIBHTTP_LINK)))

$(eval $(call include_sub_make,http_testing,testing,http_testing.mk))
