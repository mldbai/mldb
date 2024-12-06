# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# Collect source files for the `arch` library
set(LIBARCH_SOURCES
    simd_vector.cc
    demangle.cc
    tick_counter.cc
    cpuid.cc
    simd.cc
    exception.cc
    exception_handler.cc
    backtrace.cc
    format.cc
    fslock.cc
    gpgpu.cc
    environment_static.cc
    cpu_info.cc
    vm.cc
    info.cc
    rtti_utils.cc
    rt.cc
    abort.cc
    spinlock.cc
    dlopen_mutex.cc
    file_functions.cc
    wait_on_address.cc
    wakeup_fd.cc
    thread_specific.cc
)

# Add conditional source files based on architecture
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")
    list(APPEND LIBARCH_SOURCES simd_vector_avx.cc)
endif()

# Define the `arch` library
add_library(arch ${LIBARCH_SOURCES})

# Link libraries to `arch` based on conditions
target_link_libraries(arch PUBLIC dl)
if(NOT BOOST_VERSION VERSION_LESS 42)
    target_link_libraries(arch PUBLIC boost_system)
endif()

# Collect source files for the `gc` library
set(LIBGC_SOURCES
    gc_lock.cc
    shared_gc_lock.cc
)

# Define the `gc` library
add_library(gc ${LIBGC_SOURCES})

# Link libraries to `gc` based on conditions
target_link_libraries(gc PUBLIC arch)
if(UNIX AND NOT APPLE) # Equivalent to Linux check
    target_link_libraries(gc PUBLIC rt)
endif()

# Set specific compile options for a file
set_source_files_properties(simd_vector_avx.cc PROPERTIES COMPILE_OPTIONS "-mavx")

# Define the `exception_hook` library
add_library(exception_hook exception_hook.cc)
target_link_libraries(exception_hook PUBLIC arch dl)

# Uncomment the following to enable `node_exception_tracing` library
# add_library(node_exception_tracing node_exception_tracing.cc)
# target_link_libraries(node_exception_tracing PUBLIC exception_hook arch dl)

# Include additional subdirectories for testing
add_subdirectory(testing)
