# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Set default values and include paths based on the USE_PLATFORM_V8 variable.
set(USE_PLATFORM_V8 1 CACHE STRING "Use platform V8")

if(NOT V8_ROOT)
    if(APPLE)
        set(V8_ROOT /opt/homebrew/opt/v8/libexec)
    elseif (UNIX)
        get_target_triple(V8_TARGET_TRIPLE V8_ARCH V8_VENDOR V8_OS)
        message("V8_SYSTEM_NAME: ${CMAKE_TARGET_SYSTEM_NAME}")
        if(CMAKE_SYSTEM_NAME MATCHES "Linux")
            set(V8_TARGET_TRIPLE ${V8_ARCH}-linux-gnu)
            set(V8_ROOT "/usr/lib/${V8_TARGET_TRIPLE}")
            set(V8_PATHS /usr/lib/v8/libexec /usr/lib/${V8_TARGET_TRIPLE})
            message(STATUS "V8_TARGET_TRIPLE: ${V8_TARGET_TRIPLE}")
            message(STATUS "v8_root: ${V8_ROOT}")
        endif()
        message(STATUS "V8_TARGET_TRIPLE: ${V8_TARGET_TRIPLE}")
        message(STATUS "v8_root: ${V8_ROOT}")
	elseif(WIN32)
		set(V8_ROOT $ENV{ProgramFiles}/v8)
	else()
		message(FATAL_ERROR "V8_ROOT not found for platform")
	endif()
endif()

set(V8_NAMES_DEBUG v8D v8_baseD v8_base.ia32D v8_base.x64D libv8_baseD v8_baseD.lib v8_libbaseD v8_libbase.ia32D v8_libbase.x64D libv8_libbaseD v8_libbaseD.lib)
set(V8_NAMES_RELEASE v8 v8_base v8_base.ia32 v8_base.x64 libv8_base v8_base.lib v8_libbase v8_libbase.ia32 v8_libbase.x64 libv8_libbase v8_libbase.lib)
set(V8_PLATFORM_NAMES_DEBUG v8_libplatformD v8_libplatformD.a v8_libplatformD.lib)
set(V8_PLATFORM_NAMES_RELEASE v8_libplatform v8_libplatform.a v8_libplatform.lib)
set(V8_SNAPSHOT_NAMES_DEBUG v8_snapshotD libv8_snapshotD v8_snapshotD.lib)
set(V8_SNAPSHOT_NAMES_RELEASE v8_snapshot libv8_snapshot v8_snapshot.lib)
set(V8_ICU_NAMES_DEBUG icudataD icudataD.a icudataD.lib)
set(V8_ICU_NAMES_RELEASE icudata icudata.a icudata.lib)
set(V8_ICUUC_NAMES_DEBUG icuucD libicuucD)
set(V8_ICUUC_NAMES_RELEASE icuuc libicuuc)
set(V8_ICUI18N_NAMES_DEBUG icui18nD libicui18nD)
set(V8_ICUI18N_NAMES_RELEASE icui18n libicui18n)

message("V8_PATHS: ${V8_PATHS}")
message("V8_NAMES_RELEASE: ${V8_NAMES_RELEASE}")

# V8 library paths
set(V8_LIBRARY_PATH_SUFFIXES lib lib64 lib.target)

# Find include path
if(MSVC OR CMAKE_BUILD_TYPE EQUAL "Debug")
	set(V8_HEADERS v8.h v8-debug.h v8-profiler.h v8stdint.h)
else()
    if(DEFINED DISTRIB_CODENAME)
        set(V8_RELEASE_CODENAME ${DISTRIB_CODENAME})
    else()
        message(FATAL_ERROR "No distribution codename found; does /etc/lsb-release exist and define DISTRIB_CODENAME?")
    endif()

    # Define the V8 architecture based on the ARCH variable
    set(V8_ARCH_x86_64 "x64")
    set(V8_ARCH_aarch64 "arm64")
    set(V8_ARCH_arm "arm")
    set(V8_ARCH ${V8_ARCH_${ARCH}})

    if(NOT V8_ARCH)
        message(FATAL_ERROR "Couldn't find V8 architecture for ${ARCH}; set V8_ARCH_${ARCH} in mldb_js_plugin.mk")
    endif()

    # Define library dependencies and paths
    set(LIB_v8_DEPS
        "${LIB}/libv8${SO_EXTENSION}"
        "${LIB}/natives_blob.bin"
        "${LIB}/snapshot_blob.bin"
    )
    set(V8_INCLUDE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/mldb/ext/v8-cross-build-output/include")

    add_custom_command(
        OUTPUT "${CMAKE_BINARY_DIR}/${LIB}/libv8${SO_EXTENSION}"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE_DIR:mldb/ext/v8-cross-build-output/${V8_RELEASE_CODENAME}/${V8_ARCH}/libv8${SO_EXTENSION}> $<TARGET_FILE_DIR:mldb/ext/v8-cross-build-output/${V8_RELEASE_CODENAME}/${V8_ARCH}/libv8${SO_EXTENSION}>
        DEPENDS "mldb/ext/v8-cross-build-output/${V8_RELEASE_CODENAME}/${V8_ARCH}/libv8${SO_EXTENSION}"
    )

    if(NOT PREMAKE)
        add_custom_command(
            OUTPUT "${CMAKE_BINARY_DIR}/${LIB}/natives_blob.bin"
            COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE_DIR:mldb/ext/v8-cross-build-output/${V8_RELEASE_CODENAME}/${V8_ARCH}/natives_blob.bin> $<TARGET_FILE_DIR:mldb/ext/v8-cross-build-output/${V8_RELEASE_CODENAME}/${V8_ARCH}/natives_blob.bin>
            DEPENDS "mldb/ext/v8-cross-build-output/${V8_RELEASE_CODENAME}/${V8_ARCH}/natives_blob.bin"
        )
        
        add_custom_command(
            OUTPUT "${CMAKE_BINARY_DIR}/${LIB}/snapshot_blob.bin"
            COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE_DIR:mldb/ext/v8-cross-build-output/${V8_RELEASE_CODENAME}/${V8_ARCH}/snapshot_blob.bin> $<TARGET_FILE_DIR:mldb/ext/v8-cross-build-output/${V8_RELEASE_CODENAME}/${V8_ARCH}/snapshot_blob.bin>
            DEPENDS "mldb/ext/v8-cross-build-output/${V8_RELEASE_CODENAME}/${V8_ARCH}/snapshot_blob.bin"
        )
    endif()
endif()

# List of libraries linked for LIBJS
set(LIBJS_LINK
    value_description
    v8
    v8_libplatform
    arch
    utils
    types
    mldb_engine
    log
    any
    mldb_core
    rest
    rest_entity
    json_diff
    logging
    vfs
    base
    mldb_core
    mldb_builtin_base
    sql_types
    http
)

# JS plugin source files
set(JS_PLUGIN_SOURCE
    js_utils.cc
    js_value.cc
    js_plugin.cc
    js_function.cc
    js_common.cc
    dataset_js.cc
    function_js.cc
    procedure_js.cc
    mldb_js.cc
    sensor_js.cc
    mldb_v8_platform_common.cc
    mldb_v8_platform_v11.cc
    mldb_v8_platform_v12.cc
)

# Apply compile options to JS_PLUGIN_SOURCE files
foreach(source IN LISTS JS_PLUGIN_SOURCE)
    set_source_files_properties(${source} PROPERTIES COMPILE_FLAGS "-I${V8_INCLUDE_PATH} ${V8_INCLUDE_FLAGS}")
endforeach()

# Define the mldb_js_plugin library
add_library(mldb_js_plugin ${JS_PLUGIN_SOURCE})
target_link_libraries(mldb_js_plugin value_description ${LIBJS_LINK} sql_expression)
