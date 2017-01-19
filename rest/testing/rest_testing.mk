# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# rest_testing.mk
# Jeremy Barnes, 16 March 2015
# Testing for the REST functionality


ETCD_MANUAL:=$(if $(HAS_ETCD),,manual)

$(eval $(call test,link_test,link,boost timed valgrind))
$(eval $(call test,rest_collection_test,service_peer,boost timed))
$(eval $(call test,rest_collection_stress_test,service_peer,boost timed))
$(eval $(call test,service_peer_test,service_peer,boost $(ETCD_MANUAL) timed))
$(eval $(call test,service_peer_startup_test,service_peer,boost $(ETCD_MANUAL) timed))
$(eval $(call test,service_peer_process_discovery_test,service_peer runner,boost $(ETCD_MANUAL) timed))
$(eval $(call test,service_peer_wrong_port_test,service_peer,boost $(ETCD_MANUAL) timed))
$(eval $(call test,service_peer_name_clash_test,service_peer,boost $(ETCD_MANUAL) timed))
$(eval $(call test,service_peer_watch_test,service_peer,boost $(ETCD_MANUAL) timed))
$(eval $(call test,asio_peer_server_test,watch service_peer,boost $(ETCD_MANUAL) timed))
$(eval $(call test,service_peer_link_test,service_peer,boost $(ETCD_MANUAL) timed))
$(eval $(call test,service_peer_discovery_test,service_peer,boost $(ETCD_MANUAL) timed))
$(eval $(call test,etcd_discovery_test,service_peer,boost $(ETCD_MANUAL) timed))

$(eval $(call test,service_peer_fuzz_test,service_peer,boost manual))


$(eval $(call program,test_peer_runner,service_peer))

$(TESTS)/service_peer_test \
$(TESTS)/service_peer_wrong_port_test \
$(TESTS)/service_peer_process_discovery_test: \
	$(BIN)/test_peer_runner \

$(eval $(call test,rest_service_endpoint_test,rest,boost manual))
$(eval $(call test,rest_request_router_test,rest,boost))
$(eval $(call test,rest_request_binding_test,rest,boost))

