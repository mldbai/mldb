# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Pro plugins

set(LIBMLDB_PRO_PLUGIN_SOURCES
    pro_plugin.cc
    list_files_procedure.cc
    dataset_stats_procedure.cc
)

# Set compile option for pro plugin sources
foreach(source IN LISTS LIBMLDB_PRO_PLUGIN_SOURCES)
    set_source_files_properties(${source} PROPERTIES COMPILE_FLAGS "-Ipro")
endforeach()

# Create the pro plugin library
add_library(mldb_pro_plugin ${LIBMLDB_PRO_PLUGIN_SOURCES})
target_link_libraries(mldb_pro_plugin PRIVATE ml)

# Add as a built-in plugin
# The equivalent of mldb_builtin_plugin in CMake
set_target_properties(mldb_pro_plugin PROPERTIES
    OUTPUT_NAME "pro"
)

add_subdirectory(testing)
