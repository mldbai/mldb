# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

set(LIBIO_SOURCES
    asio_thread_pool.cc
    asio_timer.cc
    port_range_service.cc
    event_loop.cc
    legacy_event_loop.cc
    tcp_acceptor.cc
    tcp_socket_handler.cc
    tcp_socket.cc
    event_loop_impl.cc
    tcp_acceptor_impl.cc
    tcp_socket_handler_impl.cc
    timerfd.cc
    epoller.cc
    epoll_loop.cc
    message_loop.cc
    async_event_source.cc
    async_writer_source.cc
)

set(LIBIO_LINK logging watch types arch value_description base)

add_library(io_base ${LIBIO_SOURCES})
target_link_libraries(io_base ${LIBIO_LINK})

add_subdirectory(testing)
