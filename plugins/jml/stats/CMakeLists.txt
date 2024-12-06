# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

set(LIBSTATS_SOURCES
    auc.cc)

# Define the stats library target
add_library(stats ${LIBSTATS_SOURCES})

# Link required libraries to stats library
target_link_libraries(stats utils arch)

add_subdirectory(testing)
