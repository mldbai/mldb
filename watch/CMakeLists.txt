# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# Define the library target for 'watch'
add_library(watch
    watch.cc
)

# Link the necessary libraries to the 'watch' target
target_link_libraries(watch PRIVATE
    gc
    any
    types
    arch
    base
    value_description
)

# Include the subdirectory for tests related to 'watch'
add_subdirectory(testing)