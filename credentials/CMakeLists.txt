# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# credentials makefile
# Jeremy Barnes, 5 November 2014
# Copyright (c) 2014 mldb.ai inc.  All rights reserved.

set(LIBCREDENTIALS_SOURCES
    credentials.cc
    credential_provider.cc
)

set(LIBCREDENTIALS_LINK
    arch
    utils
    types
    value_description
)

add_library(credentials ${LIBCREDENTIALS_SOURCES})
target_link_libraries(credentials ${LIBCREDENTIALS_LINK})
