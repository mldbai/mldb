# Set sources based on architecture
set(HIGHWAYHASH_SOURCE_x86_64 hh_avx2.cc hh_sse41.cc)
set(HIGHWAYHASH_SOURCE_arm64 hh_neon.cc)
set(HIGHWAYHASH_SOURCE_aarch64 ${HIGHWAYHASH_SOURCE_arm64})

# Set compilation flags based on the compiler
#set(HIGHWAYHASH_FLAGS_clang -Wno-unused-private-field)
#set(HIGHWAYHASH_FLAGS_gcc "")

# Determine the appropriate sources based on the architecture
set(HIGHWAYHASH_SOURCE 
    highwayhash.cc
    highwayhash/highwayhash/sip_hash.cc
    highwayhash/highwayhash/hh_portable.cc
    highwayhash/highwayhash/arch_specific.cc
    highwayhash/highwayhash/instruction_sets.cc
    highwayhash/highwayhash/nanobenchmark.cc
    highwayhash/highwayhash/os_specific.cc
    highwayhash/highwayhash/c_bindings.cc
    ${HIGHWAYHASH_SOURCE_${ARCH}}
)

# Set compile options for the source files
set_source_files_properties(${HIGHWAYHASH_SOURCE} PROPERTIES
    INCLUDE_DIRECTORIES
        "${CMAKE_CURRENT_SOURCE_DIR}/highwayhash"
    COMPILE_FLAGS
        ""
)


#set_compile_options("${HIGHWAYHASH_SOURCE}" "-I${} ${HIGHWAYHASH_FLAGS_${toolchain}} -fno-sanitize=undefined")

# Additional architecture-specific settings
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")
    set_compile_options("highwayhash/hh_sse41.cc" "-msse4.1")
    set_compile_options("highwayhash/hh_avx2.cc" "-mavx2")
endif()

# Common include paths for specific source files
#set_compile_options("highwayhash/scalar_highway_tree_hash.cc" "-I${CWD}")
#set_compile_options("highwayhash/sip_hash.cc" "-I${CWD}")
#set_compile_options("../highwayhash.cc" "-I${CWD}")

# Define the library target
add_library(highwayhash ${HIGHWAYHASH_SOURCE})
