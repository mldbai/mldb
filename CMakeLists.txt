cmake_minimum_required(VERSION 3.30)

project(mldb)

# Set the C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Boost REQUIRED COMPONENTS program_options unit_test_framework)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${Boost_INCLUDE_DIRS})

enable_testing()

# Include our testing functions and build definitions
add_subdirectory(jml-build)

add_subdirectory(ext)
add_subdirectory(arch)
add_subdirectory(base)
#add_subdirectory(types)
#add_subdirectory(watch)
#add_subdirectory(logging)
#add_subdirectory(io)
#add_subdirectory(http)
#add_subdirectory(vfs)
#add_subdirectory(block)
#add_subdirectory(utils)
#add_subdirectory(credentials)
#add_subdirectory(rest)
#add_subdirectory(vfs_handlers)
#add_subdirectory(sql)
#add_subdirectory(core)
#add_subdirectory(engine)
#add_subdirectory(builtin)
#add_subdirectory(plugins)
#add_subdirectory(server)
#add_subdirectory(sdk)
#add_subdirectory(testing)

