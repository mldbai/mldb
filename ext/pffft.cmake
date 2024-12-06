# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

set(PFFFT_SOURCE pffft.c fftpack.c)
list(TRANSFORM PFFFT_SOURCE PREPEND "pffft/")

# Detect FFTW3 availability
find_path(FFTW3_INCLUDE_DIR NAMES fftw3.h)
if(FFTW3_INCLUDE_DIR)
    add_compile_definitions(HAVE_FFTW)
endif()

add_library(pffft STATIC ${PFFFT_SOURCE})

# Determine the necessary libraries for test_pffft
set(TEST_PFFFT_LIBRARIES pffft)
if(FFTW3_INCLUDE_DIR)
    list(APPEND TEST_PFFFT_LIBRARIES fftw3f)
endif()

#add_mldb_test(test_pffft "${TEST_PFFFT_LIBRARIES}" "")
