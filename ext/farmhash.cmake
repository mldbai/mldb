# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

#input: FARMHASH_SOURCE = src/farmhash.cc

# Set the FARMHASH_INCLUDE_FILES variable
set(FARMHASH_INCLUDE_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/farmhash.h
)

# Adding the farmhash library target with its source file
add_library(farmhash STATIC src/farmhash.cc)

# Specify include directories for the farmhash library
target_include_directories(farmhash PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
