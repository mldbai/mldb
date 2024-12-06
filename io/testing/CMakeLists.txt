# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

add_mldb_test(asio_timer_test "io_base;watch;types" "boost")
add_mldb_test(async_writer_bench "io_base;arch;types" "boost;manual")
add_mldb_test(epoll_test "io_base;arch;base" "boost")
add_mldb_test(epoller_test "io_base" "boost")
add_mldb_test(timerfd_test "io_base" "boost")
add_mldb_test(message_channel_test "io_base;arch;types" "boost")
add_mldb_test(message_loop_test "io_base;arch;types" "boost")
