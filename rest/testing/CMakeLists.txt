# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# rest_testing.cmake
# Jeremy Barnes, 16 March 2015
# Testing for the REST functionality

# Handling manual flag based on HAS_ETCD
set(ETCD_MANUAL "")
if(NOT HAS_ETCD)
    set(ETCD_MANUAL "manual")
endif()

# Defining extra libraries for REST testing
set(REST_TESTING_EXTRA_LIBS watch any arch value_description types rest ${REST_INDIRECT_DEPS} rest_entity ${REST_ENTITY_INDIRECT_DEPS} gc)

# Define tests
add_mldb_test(link_test "link;${LINK_INDIRECT_DEPS};watch" "boost;timed;valgrind")
add_mldb_test(rest_collection_test "service_peer;${SERVICE_PEER_INDIRECT_DEPS};${REST_TESTING_EXTRA_LIBS}" "boost;timed")
add_mldb_test(rest_collection_stress_test "service_peer;${SERVICE_PEER_INDIRECT_DEPS};${REST_TESTING_EXTRA_LIBS}" "boost;timed")
add_mldb_test(service_peer_test "service_peer;${SERVICE_PEER_INDIRECT_DEPS};${REST_TESTING_EXTRA_LIBS}" "boost;${ETCD_MANUAL};timed")
add_mldb_test(service_peer_startup_test "service_peer;${SERVICE_PEER_INDIRECT_DEPS};${REST_TESTING_EXTRA_LIBS}" "boost;${ETCD_MANUAL};timed")
add_mldb_test(service_peer_process_discovery_test "service_peer;${SERVICE_PEER_INDIRECT_DEPS};runner;${REST_TESTING_EXTRA_LIBS}" "boost;${ETCD_MANUAL};timed")
add_mldb_test(service_peer_wrong_port_test "service_peer;${SERVICE_PEER_INDIRECT_DEPS};${REST_TESTING_EXTRA_LIBS}" "boost;${ETCD_MANUAL};timed")
add_mldb_test(service_peer_name_clash_test "service_peer;${SERVICE_PEER_INDIRECT_DEPS};${REST_TESTING_EXTRA_LIBS}" "boost;${ETCD_MANUAL};timed")
add_mldb_test(service_peer_watch_test "service_peer;${SERVICE_PEER_INDIRECT_DEPS};${REST_TESTING_EXTRA_LIBS}" "boost;${ETCD_MANUAL};timed")
add_mldb_test(asio_peer_server_test "watch;service_peer;${SERVICE_PEER_INDIRECT_DEPS};${REST_TESTING_EXTRA_LIBS}" "boost;${ETCD_MANUAL};timed")
add_mldb_test(service_peer_link_test "service_peer;${SERVICE_PEER_INDIRECT_DEPS};${REST_TESTING_EXTRA_LIBS}" "boost;${ETCD_MANUAL};timed")
add_mldb_test(service_peer_discovery_test "service_peer;${SERVICE_PEER_INDIRECT_DEPS};${REST_TESTING_EXTRA_LIBS}" "boost;${ETCD_MANUAL};timed")
add_mldb_test(etcd_discovery_test "service_peer;${SERVICE_PEER_INDIRECT_DEPS};${REST_TESTING_EXTRA_LIBS}" "boost;${ETCD_MANUAL};timed")

add_mldb_test(service_peer_fuzz_test "service_peer;${SERVICE_PEER_INDIRECT_DEPS};${REST_TESTING_EXTRA_LIBS}" "boost;manual")

# Define program
add_executable(test_peer_runner test_peer_runner.cc)
target_link_libraries(test_peer_runner service_peer ${SERVICE_PEER_INDIRECT_DEPS} ${REST_TESTING_EXTRA_LIBS} ${SERVICE_PEER_INDIRECT_DEPS})

# Define additional tests
add_mldb_test(rest_service_endpoint_test "rest;${REST_TESTING_EXTRA_LIBS}" "boost;manual")
add_mldb_test(rest_request_router_test "rest;${REST_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(rest_request_binding_test "rest;${REST_TESTING_EXTRA_LIBS}" "boost")
