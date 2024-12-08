# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

include(parserutils.cmake)
include(hubbub.cmake)
include(tinyxpath.cmake)
include(uap-cpp.cmake)


add_library(html STATIC UaParser.cpp) # Add the source file

# Link the plugin library with necessary dependencies
target_link_libraries(html PRIVATE uap yaml-cpp)
