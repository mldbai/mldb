# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Define the source files for the ZSTD library
set(ZSTD_SOURCE
    compress/zstd_compress_superblock.c
    compress/zstdmt_compress.c
    compress/zstd_double_fast.c
    compress/zstd_fast.c
    compress/zstd_compress_sequences.c
    compress/zstd_ldm.c
    compress/hist.c
    compress/zstd_compress.c
    compress/zstd_lazy.c
    compress/zstd_compress_literals.c
    compress/huf_compress.c
    compress/zstd_opt.c
    compress/fse_compress.c
    dictBuilder/cover.c
    dictBuilder/divsufsort.c
    dictBuilder/fastcover.c
    dictBuilder/zdict.c
    decompress/zstd_ddict.c
    decompress/huf_decompress.c
    decompress/zstd_decompress.c
    decompress/zstd_decompress_block.c
    legacy/zstd_v05.c
    legacy/zstd_v01.c
    legacy/zstd_v06.c
    legacy/zstd_v02.c
    legacy/zstd_v07.c
    legacy/zstd_v03.c
    legacy/zstd_v04.c
    common/entropy_common.c
    common/fse_decompress.c
    common/debug.c
    common/xxhash.c
    common/pool.c
    common/threading.c
    common/zstd_common.c
    common/error_private.c
    deprecated/zbuff_common.c
    deprecated/zbuff_decompress.c
    deprecated/zbuff_compress.c
)

list(TRANSFORM ZSTD_SOURCE PREPEND "zstd/lib/")

# Define the source files for the ZSTD binary
set(ZSTD_BINARY_SOURCE
    zstdcli.c
    fileio.c
    datagen.c
    dibio.c
    util.c
    zstdcli_trace.c
    benchzstd.c
    benchfn.c
    timefn.c
    fileio_asyncio.c
    lorem.c
)

list(TRANSFORM ZSTD_BINARY_SOURCE PREPEND "zstd/programs/")

# Determine the flags based on the toolchain being used
#set(ZSTD_GCC_FLAGS "-Wno-deprecated-declarations -Wno-maybe-uninitialized")
#set(ZSTD_CLANG_FLAGS "-Wno-deprecated-declarations -Wno-maybe-uninitialized -Wno-unknown-warning-option -Wno-unused-but-set-variable")

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    set(ZSTD_FLAGS "${ZSTD_GCC_FLAGS}")
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    set(ZSTD_FLAGS "${ZSTD_CLANG_FLAGS}")
endif()

set_source_files_properties(
    ${ZSTD_SOURCE} PROPERTIES
    INCLUDE_DIRECTORIES 
        ${CMAKE_CURRENT_SOURCE_DIR}/zstd/include
        ${CMAKE_CURRENT_SOURCE_DIR}/zstd/lib
        ${CMAKE_CURRENT_SOURCE_DIR}/zstd/lib/common
        ${CMAKE_CURRENT_SOURCE_DIR}/zstd/lib/dictBuilder
        ${CMAKE_CURRENT_SOURCE_DIR}/zstd/lib/legacy
    COMPILE_FLAGS
        -DZSTD_LEGACY_SUPPORT=1
        ${ZSTD_FLAGS}
)

add_library(zstd ${ZSTD_SOURCE})

add_executable(zstdcli ${ZSTD_BINARY_SOURCE})
target_link_libraries(zstdcli PRIVATE zstd)
