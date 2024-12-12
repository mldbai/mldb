add_subdirectory(xxhash)

include(cryptopp.cmake)
include(highwayhash.cmake)
include(zstd.cmake)
include(pffft.cmake)
include(easyexif.cmake)
include(fasttext.cmake)
include(hoedown.cmake)

# Export this for sub CMakeLists.txt files
set(MLDB_EXT_DIR ${CMAKE_CURRENT_SOURCE_DIR})

set(BUILD_SHARED_LIBS ON)
set(RE2_BUILD_TESTING OFF)


add_subdirectory(utfcpp)
add_subdirectory(re2)
add_subdirectory(googleurl)
add_subdirectory(cityhash)
add_subdirectory(lz4)
add_subdirectory(edlib)
add_subdirectory(tinyxml2)
add_subdirectory(judy)
add_subdirectory(svdlibc)

set(BUILD_TESTS OFF)
set(BUILD_CLI OFF)
add_subdirectory(libgit2)

add_subdirectory(sqlite)

set(ABSL_PROPAGATE_CXX_STD ON)
set(ABSL_ENABLE_INSTALL OFF)
add_subdirectory(abseil-cpp)

set(BUILD_TESTS OFF)
set(ENABLE_INSTALL OFF)
add_subdirectory(s2geometry)
