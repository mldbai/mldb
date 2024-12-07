# The exclusion lists for different architectures
set(CRYPTOPP_EXCLUDE_aarch64 "chacha_avx.cpp" "donna_sse.cpp" "sse_simd.cpp")
set(CRYPTOPP_EXCLUDE_x86_64 "neon_simd.cpp")
set(CRYPTOPP_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}" CACHE INTERNAL "Cryptopp include directory")

# Collect all source files and apply exclusions based on the architecture
file(GLOB CRYPTOPP_FILES "${CMAKE_CURRENT_SOURCE_DIR}/cryptopp/*.cpp")
list(FILTER CRYPTOPP_FILES EXCLUDE REGEX ".*/pch\.cpp|.*/simple\.cpp|.*/adhoc\.cpp|.*/test\.cpp|.*/bench1\.cpp|.*/bench2\.cpp|.*/bench3\.cpp|.*/datatest\.cpp|.*/dlltest\.cpp|.*/fipsalgt\.cpp|.*/validat0\.cpp|.*/validat1\.cpp|.*/validat2\.cpp|.*/validat3\.cpp|.*/validat4\.cpp|.*/validat5\.cpp|.*/validat6\.cpp|.*/validat7\.cpp|.*/validat8\.cpp|.*/validat9\.cpp|.*/validat10\.cpp|.*/regtest1\.cpp|.*/regtest2\.cpp|.*/regtest3\.cpp|.*/regtest4\.cpp")
#list(REMOVE_ITEM CRYPTOPP_FILES "${CMAKE_CURRENT_SOURCE_DIR}/cryptopp/cryptlib.cpp" "${CMAKE_CURRENT_SOURCE_DIR}/cryptopp/cpu.cpp" "${CMAKE_CURRENT_SOURCE_DIR}/cryptopp/integer.cpp")
#list(REMOVE_ITEM CRYPTOPP_FILES "${CMAKE_CURRENT_SOURCE_DIR}/cryptopp/cryptlib.cpp" "${CMAKE_CURRENT_SOURCE_DIR}/cryptopp/cpu.cpp")

message(CRYPTOPP_FILES: ${CRYPTOPP_FILES})
message("CMAKE_SYSTEM_PROCESSOR: ${CMAKE_SYSTEM_PROCESSOR}")

if(CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
    set(CRYPTOPP_ARCH "aarch64")
else()
    set(CRYPTOPP_ARCH ${CMAKE_SYSTEM_PROCESSOR})
endif()

message("CRYPTOPP_ARCH: ${CRYPTOPP_ARCH}")

# Exclude specific files based on the architecture
foreach(exclusion ${CRYPTOPP_EXCLUDE_${CRYPTOPP_ARCH}})
    list(REMOVE_ITEM CRYPTOPP_FILES "${CMAKE_CURRENT_SOURCE_DIR}/${exclusion}")
endforeach()

# Compiler flags for specific files based on architecture
if(CRYPTOPP_ARCH STREQUAL "x86_64")
    set_compile_options("aria_simd.cpp" "-msse3")
    set_compile_options("blake2b_simd.cpp" "-msse4.1")
    set_compile_options("blake2s_simd.cpp" "-msse4.1")
    set_compile_options("chacha_avx.cpp" "-mavx2")
    set_compile_options("chacha_simd.cpp" "-msse2")
    set_compile_options("cham_simd.cpp" "-msse3")
    set_compile_options("crc_simd.cpp" "-msse4.2")
    set_compile_options("donna_sse.cpp" "-msse2")
    set_compile_options("gcm_simd.cpp" "-msse3 -mpclmul")
    set_compile_options("gf2n_simd.cpp" "-mpclmul")
    set_compile_options("keccak_simd.cpp" "-msse3")
    set_compile_options("lea_simd.cpp" "-msse3")
    set_compile_options("rijndael_simd.cpp" "-msse4.1 -maes")
    set_compile_options("sha_simd.cpp" "-msse4.2 -msha")
    set_compile_options("shacal2_simd.cpp" "-msse4.2 -msha")
    set_compile_options("simon128_simd.cpp" "-msse3")
    set_compile_options("sm4_simd.cpp" "-msse3")
    set_compile_options("speck128_simd.cpp" "-msse3")
    set_compile_options("sse_simd.cpp" "-msse3")
endif()


function(set_compile_options source options)
    set_source_files_properties("${CMAKE_CURRENT_SOURCE_DIR}/cryptopp/${source}" PROPERTIES COMPILE_OPTIONS "${options}")
endfunction()

if(CRYPTOPP_ARCH STREQUAL "aarch64")
    set_compile_options("aria_simd.cpp" "-march=armv8-a")
    set_compile_options("blake2b_simd.cpp" "-march=armv8-a")
    set_compile_options("blake2s_simd.cpp" "-march=armv8-a")
    set_compile_options("chacha_simd.cpp" "-march=armv8-a")
    set_compile_options("cham_simd.cpp" "-march=armv8-a")
    set_compile_options("crc_simd.cpp" "-march=armv8-a+crc")
    set_compile_options("gcm_simd.cpp" "-march=armv8-a+crypto+aes")
    set_compile_options("gf2n_simd.cpp" "-march=armv8-a+crypto+aes")
    set_compile_options("keccak_simd.cpp" "-march=armv8-a")
    set_compile_options("lea_simd.cpp" "-march=armv8-a")
    set_compile_options("rijndael_simd.cpp" "-march=armv8-a+crypto+aes")
    set_compile_options("neon_simd.cpp" "-march=armv8-a")
    set_compile_options("sha_simd.cpp" "-march=armv8-a+crypto")
    set_compile_options("shacal2_simd.cpp" "-march=armv8-a+crypto+aes")
    set_compile_options("simon128_simd.cpp" "-march=armv8-a")
    set_compile_options("sm4_simd.cpp" "-march=armv8-a")
    set_compile_options("speck128_simd.cpp" "-march=armv8-a")
endif()

# Add the cryptopp library
add_library(cryptopp STATIC ${CRYPTOPP_FILES})

# Include directory
target_include_directories(cryptopp PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/mldb/ext)
