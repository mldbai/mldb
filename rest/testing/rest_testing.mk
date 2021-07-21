# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# rest_testing.mk
# Jeremy Barnes, 16 March 2015
# Testing for the REST functionality


ETCD_MANUAL:=$(if $(HAS_ETCD),,manual)

REST_TESTING_EXTRA_LIBS:=watch any arch value_description types rest $(REST_INDIRECT_DEPS) rest_entity $(REST_ENTITY_INDIRECT_DEPS) gc

$(eval $(call test,link_test,link $(LINK_INDIRECT_DEPS) watch,boost timed valgrind))
$(eval $(call test,rest_collection_test,service_peer $(SERVICE_PEER_INDIRECT_DEPS) $(REST_TESTING_EXTRA_LIBS),boost timed))
$(eval $(call test,rest_collection_stress_test,service_peer $(SERVICE_PEER_INDIRECT_DEPS) $(REST_TESTING_EXTRA_LIBS),boost timed))
$(eval $(call test,service_peer_test,service_peer $(SERVICE_PEER_INDIRECT_DEPS) $(REST_TESTING_EXTRA_LIBS),boost $(ETCD_MANUAL) timed))
$(eval $(call test,service_peer_startup_test,service_peer $(SERVICE_PEER_INDIRECT_DEPS) $(REST_TESTING_EXTRA_LIBS),boost $(ETCD_MANUAL) timed))
$(eval $(call test,service_peer_process_discovery_test,service_peer $(SERVICE_PEER_INDIRECT_DEPS) runner $(REST_TESTING_EXTRA_LIBS),boost $(ETCD_MANUAL) timed))
$(eval $(call test,service_peer_wrong_port_test,service_peer $(SERVICE_PEER_INDIRECT_DEPS) $(REST_TESTING_EXTRA_LIBS),boost $(ETCD_MANUAL) timed))
$(eval $(call test,service_peer_name_clash_test,service_peer $(SERVICE_PEER_INDIRECT_DEPS) $(REST_TESTING_EXTRA_LIBS),boost $(ETCD_MANUAL) timed))
$(eval $(call test,service_peer_watch_test,service_peer $(SERVICE_PEER_INDIRECT_DEPS) $(REST_TESTING_EXTRA_LIBS),boost $(ETCD_MANUAL) timed))
$(eval $(call test,asio_peer_server_test,watch service_peer $(SERVICE_PEER_INDIRECT_DEPS) $(REST_TESTING_EXTRA_LIBS),boost $(ETCD_MANUAL) timed))
$(eval $(call test,service_peer_link_test,service_peer $(SERVICE_PEER_INDIRECT_DEPS) $(REST_TESTING_EXTRA_LIBS),boost $(ETCD_MANUAL) timed))
$(eval $(call test,service_peer_discovery_test,service_peer $(SERVICE_PEER_INDIRECT_DEPS) $(REST_TESTING_EXTRA_LIBS),boost $(ETCD_MANUAL) timed))
$(eval $(call test,etcd_discovery_test,service_peer $(SERVICE_PEER_INDIRECT_DEPS) $(REST_TESTING_EXTRA_LIBS),boost $(ETCD_MANUAL) timed))

$(eval $(call test,service_peer_fuzz_test,service_peer $(SERVICE_PEER_INDIRECT_DEPS) $(REST_TESTING_EXTRA_LIBS),boost manual))


$(eval $(call program,test_peer_runner,service_peer $(SERVICE_PEER_INDIRECT_DEPS) $(REST_TESTING_EXTRA_LIBS) $(SERVICE_PEER_INDIRECT_DEPS)))

$(TESTS)/service_peer_test \
$(TESTS)/service_peer_wrong_port_test \
$(TESTS)/service_peer_process_discovery_test: \
	$(BIN)/test_peer_runner \

$(eval $(call test,rest_service_endpoint_test,rest $(REST_TESTING_EXTRA_LIBS),boost manual))
$(eval $(call test,rest_request_router_test,rest $(REST_TESTING_EXTRA_LIBS),boost))
$(eval $(call test,rest_request_binding_test,rest $(REST_TESTING_EXTRA_LIBS),boost))

