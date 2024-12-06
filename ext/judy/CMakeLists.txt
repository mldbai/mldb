# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Sources for the Judy library
set(LIBJUDY_SOURCES
    JudyLByCount.cc
    JudyLCreateBranch.cc
    JudyLCascade.cc
    JudyLCount.cc
    judy_malloc_allocator.cc
    JudyLDecascade.cc
    JudyLDel.cc
    JudyLFirst.cc
    JudyLFreeArray.cc
    JudyLGet.cc
    JudyLIns.cc
    JudyLInsArray.cc
    JudyLInsertBranch.cc
    JudyLMallocIF.cc
    JudyLMemActive.cc
    JudyLMemUsed.cc
    JudyLNext.cc
    JudyLNextEmpty.cc
    JudyLPrev.cc
    JudyLPrevEmpty.cc
    JudyLTables.cc
    JudyLTablesGen.cc
    j__udyLGet.cc
)

# Compilation options for GCC 4.9
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 4.9)
    target_compile_options(judy PRIVATE -fno-strict-aliasing -Wno-array-bounds)
    if(CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 5)
        target_compile_options(judy PRIVATE -Wno-stringop-overflow)
    endif()
endif()

# Create the Judy library
add_library(judy ${LIBJUDY_SOURCES})
