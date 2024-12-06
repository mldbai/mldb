# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Build the logging library
add_library(logging logging.cc)
target_link_libraries(logging PRIVATE types arch)

# Include the testing subdirectory
add_subdirectory(testing)
