include(cryptopp.cmake)
include(highwayhash.cmake)

# Export this for sub CMakeLists.txt files
set(MLDB_EXT_DIR ${CMAKE_CURRENT_SOURCE_DIR})

set(BUILD_SHARED_LIBS ON)
set(RE2_BUILD_TESTING OFF)


add_subdirectory(utfcpp)
add_subdirectory(re2)
add_subdirectory(googleurl)
add_subdirectory(cityhash)
