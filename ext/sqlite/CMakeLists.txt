# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Support functions for javascript

# Set compile options for specific files when using GCC 4.7 and above
set_source_files_properties(
    sqlite3.c shell.c
    PROPERTIES COMPILE_OPTIONS "-Wno-array-bounds;-Wno-unused-const-variable;-Wno-misleading-indentation;$<CXX_COMPILER_VERSION:4.7>:-Wno-stringop-overread"
)

# Create a library named sqlite-mldb with the specified source files
add_library(sqlite-mldb
    sqlite3ppext.cc
    sqlite3pp.cc
    sqlite3.c
)

# Create an executable named sqliteShell that links against the sqlite-mldb library
add_executable(sqliteShell shell.c)
target_link_libraries(sqliteShell PRIVATE sqlite-mldb)
