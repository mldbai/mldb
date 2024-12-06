# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

set(LIBHTTP_SOURCES
    http_socket_handler.cc 
    http_header.cc 
    http_parsers.cc 
    http_rest_proxy.cc 
    curl_wrapper.cc 
    http_client.cc 
    http_client_callbacks.cc 
    http_request.cc 
    http_client_impl.cc 
    http_client_impl_v1.cc
)

set(LIBHTTP_LINK_LIBRARIES 
    curl 
    io_base 
    arch 
    types 
    Boost::system 
    value_description 
    ${STD_FILESYSTEM_LIBNAME} 
    cityhash 
    watch 
    base
)

add_library(http ${LIBHTTP_SOURCES})
target_link_libraries(http ${LIBHTTP_LINK_LIBRARIES})

add_subdirectory(testing)
