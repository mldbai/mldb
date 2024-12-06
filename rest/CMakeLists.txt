# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

add_subdirectory(opstats)

set(LIBREST_SOURCES 
    rest_request.cc 
    rest_request_router.cc 
    rest_request_binding.cc 
    rest_request_params.cc 
    in_process_rest_connection.cc 
    rest_service_endpoint.cc 
    http_rest_endpoint.cc 
    http_rest_service.cc 
    cancellation_exception.cc
)

set(LIBLINK_SOURCES 
    call_me_back.cc 
    link.cc
)

set(LIBREST_ENTITY_SOURCES 
    rest_entity.cc 
    rest_collection.cc 
    collection_config_store.cc 
    poly_collection.cc
)

set(LIBSERVICE_PEER_SOURCES 
    service_peer.cc 
    remote_peer.cc 
    etcd_client.cc 
    peer_message.cc 
    peer_discovery.cc 
    etcd_peer_discovery.cc 
    peer_connection.cc 
    asio_peer_server.cc 
    asio_peer_connection.cc 
    standalone_peer_server.cc 
    peer_info.cc 
    event_service.cc
)

set(REST_INDIRECT_DEPS http value_description cityhash io_base vfs db any googleurl)
set(LINK_INDIRECT_DEPS value_description arch gc types any)
set(REST_ENTITY_INDIRECT_DEPS value_description types arch watch rest vfs)
set(SERVICE_PEER_INDIRECT_DEPS 
    arch value_description types watch log http io_base any utils logging link 
    ${LINK_INDIRECT_DEPS} rest_entity ${REST_ENTITY_INDIRECT_DEPS}
)

add_library(rest ${LIBREST_SOURCES})
target_link_libraries(rest arch types utils log ${REST_INDIRECT_DEPS})

add_library(link ${LIBLINK_SOURCES})
target_link_libraries(link watch ${LINK_INDIRECT_DEPS})

add_library(rest_entity ${LIBREST_ENTITY_SOURCES})
target_link_libraries(rest_entity gc link any json_diff ${REST_ENTITY_INDIRECT_DEPS})

add_library(service_peer ${LIBSERVICE_PEER_SOURCES})
target_link_libraries(service_peer rest gc link rest_entity ${SERVICE_PEER_INDIRECT_DEPS})

add_subdirectory(testing)
