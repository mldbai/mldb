# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Add library target for lz4
add_library(lz4 lz4.c lz4hc.c lz4frame.c)
target_link_libraries(lz4 PUBLIC xxhash)

# NOTE: the source to this program is under the GPL, and so we can ONLY
# build a program; we can't copy the code or make it a library

# Specify sources for GPL command line program
set(LZ4_GPL_COMMAND_LINE_SOURCES lz4cli.c bench.c datagen.c lz4io.c)

# Set compile options
add_compile_options(-Imldb/ext/xxhash)

# Add executable for the lz4cli program
add_executable(lz4cli ${LZ4_GPL_COMMAND_LINE_SOURCES})
target_link_libraries(lz4cli PRIVATE lz4 xxhash)
