# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

LIBIO_SOURCES := \
	asio_thread_pool.cc \
	asio_timer.cc \
	port_range_service.cc \
	event_loop.cc \
	legacy_event_loop.cc \
	tcp_acceptor.cc \
	tcp_socket_handler.cc \
	tcp_socket.cc \
	event_loop_impl.cc \
	tcp_acceptor_impl.cc \
	tcp_socket_handler_impl.cc \
	epoller.cc \
	epoll_loop.cc \
	message_loop.cc \
	async_event_source.cc \
	async_writer_source.cc \

LIBIO_LINK := logging watch jsoncpp

$(eval $(call library,io_base,$(LIBIO_SOURCES),$(LIBIO_LINK)))

$(eval $(call include_sub_make,io_testing,testing,io_testing.mk))
