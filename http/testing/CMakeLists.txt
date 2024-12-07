# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

set(HTTP_TESTING_EXTRA_LIBS
    arch
    types
    value_description
    io_base
)

add_mldb_test(http_header_test "http;${HTTP_TESTING_EXTRA_LIBS}" "boost;manual")
add_mldb_test(http_parsers_test "http;${HTTP_TESTING_EXTRA_LIBS}" "boost;valgrind")
add_mldb_test(tcp_acceptor_test+http "http;${HTTP_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(tcp_acceptor_threaded_test+http "http;${HTTP_TESTING_EXTRA_LIBS}" "boost")
add_executable(http_service_bench http_service_bench.cc)
target_link_libraries(http_service_bench PRIVATE Boost::program_options http ${HTTP_TESTING_EXTRA_LIBS})
add_library(test_services STATIC test_http_services.cc)
target_link_libraries(test_services PRIVATE http ${HTTP_TESTING_EXTRA_LIBS})
add_executable(http_client_bench http_client_bench.cc)
target_link_libraries(http_client_bench PRIVATE Boost::program_options http test_services ${HTTP_TESTING_EXTRA_LIBS})
# add_mldb_test(http_client_test "http;test_services;${HTTP_TESTING_EXTRA_LIBS}" "boost") # manual: failing
add_mldb_test(http_client_test2 "http;test_services;${HTTP_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(http_client_stress_test "http;test_services;${HTTP_TESTING_EXTRA_LIBS}" "boost;manual")  # manual: too long
add_mldb_test(http_client_online_test "http;${HTTP_TESTING_EXTRA_LIBS}" "boost;manual")

# The following tests needs to be adapted to the new network code:
# add_mldb_test(endpoint_unit_test "http" "boost")
# add_mldb_test(test_active_endpoint_nothing_listening "http" "boost;manual")
# add_mldb_test(test_active_endpoint_not_responding "http" "boost;manual")
# add_mldb_test(test_endpoint_ping_pong "http" "boost;manual")
# add_mldb_test(test_endpoint_connection_speed "http" "boost;manual")
# add_mldb_test(test_endpoint_accept_speed "http" "boost")
# add_mldb_test(endpoint_closed_connection_test "http" "boost")
# add_mldb_test(http_long_header_test "http" "boost;manual")
