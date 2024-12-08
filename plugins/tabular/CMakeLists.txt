# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Behavioral dataset plugin
set(LIBMLDB_TABULAR_PLUGIN_SOURCES
    tabular_dataset.cc
    frozen_column.cc
    frozen_tables.cc
    string_frozen_column.cc
    column_types.cc
    tabular_dataset_column.cc
    tabular_dataset_chunk.cc
    transducer.cc
)

set(LIBMLDB_TABULAR_PLUGIN_LINK
    block zstd sql_expression mldb_engine mldb_core value_description arch types progress base vfs log rest
)

# Add library
add_library(mldb_tabular_plugin ${LIBMLDB_TABULAR_PLUGIN_SOURCES})
target_link_libraries(mldb_tabular_plugin ${LIBMLDB_TABULAR_PLUGIN_LINK})

# Include subdirectory for tabular testing
add_subdirectory(testing)
