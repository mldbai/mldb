# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# rest.mk
# Jeremy Barnes, 16 March 2015
# Copyright (c) 2015 mldb.ai inc.  All rights reserved.

LIBREST_SOURCES := \
	rest_request.cc \
	rest_request_router.cc \
	rest_request_binding.cc \
	rest_request_params.cc \
	in_process_rest_connection.cc \
	rest_service_endpoint.cc \
	http_rest_endpoint.cc \
	http_rest_service.cc \
	cancellation_exception.cc \

LIBLINK_SOURCES := \
	call_me_back.cc \
	link.cc


LIBREST_ENTITY_SOURCES := \
	rest_entity.cc \
	rest_collection.cc \
	collection_config_store.cc \
	poly_collection.cc \

LIBSERVICE_PEER_SOURCES := \
	service_peer.cc \
	remote_peer.cc \
	etcd_client.cc \
	peer_message.cc \
	peer_discovery.cc \
	etcd_peer_discovery.cc \
	peer_connection.cc \
	asio_peer_server.cc \
	asio_peer_connection.cc \
	standalone_peer_server.cc \
	peer_info.cc \


$(eval $(call library,rest,$(LIBREST_SOURCES),services log))
$(eval $(call library,link,$(LIBLINK_SOURCES),watch))
$(eval $(call library,rest_entity,$(LIBREST_ENTITY_SOURCES),services gc link any json_diff))
$(eval $(call library,service_peer,$(LIBSERVICE_PEER_SOURCES),rest services gc link rest_entity))


$(eval $(call include_sub_make,rest_testing,testing))
