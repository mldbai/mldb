# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

LIBIO_SOURCES := \
	asio_thread_pool.cc \
	asio_timer.cc \
	port_range_service.cc \
	event_loop.cc \
	tcp_acceptor.cc \
	tcp_socket_handler.cc \
	tcp_socket.cc \
	event_loop_impl.cc \
	tcp_acceptor_impl.cc \
	tcp_socket_handler_impl.cc \

LIBIO_LINK :=

$(eval $(call library,io,$(LIBIO_SOURCES),$(LIBIO_LINK)))
