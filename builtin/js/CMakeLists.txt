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
	set(V8_HEADERS v8.h v8stdint.h)
endif()

find_path(V8_INCLUDE_DIR ${V8_HEADERS}
	PATHS ${V8_PATHS}
	PATH_SUFFIXES include
	DOC "Google V8 JavaScript Engine Headers"
)

message(STATUS "V8_INCLUDE_DIR: ${V8_INCLUDE_DIR}")

# Find V8 base library release
find_library(V8_LIBRARY_RELEASE
	NAMES ${V8_NAMES_RELEASE}
	PATHS ${V8_PATHS} ${V8_PATHS_RELEASE}
	PATH_SUFFIXES ${V8_LIBRARY_PATH_SUFFIXES}
	DOC "Google V8 JavaScript Engine Library (Release)"
)

# Find V8 base library debug
find_library(V8_LIBRARY_DEBUG
	NAMES ${V8_NAMES_DEBUG}
	PATHS ${V8_PATHS} ${V8_PATHS_DEBUG}
	PATH_SUFFIXES ${V8_LIBRARY_PATH_SUFFIXES}
	DOC "Google V8 JavaScript Engine Library (Debug)"
)

# Find V8 platform library release
find_library(V8_PLATFORM_LIBRARY_RELEASE
	NAMES ${V8_PLATFORM_NAMES_RELEASE}
	PATHS ${V8_PATHS} ${V8_PATHS_RELEASE}
	PATH_SUFFIXES ${V8_LIBRARY_PATH_SUFFIXES}
	DOC "Google V8 JavaScript Engine Library Platform (Release)"
)

# Find V8 platform library debug
find_library(V8_PLATFORM_LIBRARY_DEBUG
	NAMES ${V8_PLATFORM_NAMES_DEBUG}
	PATHS ${V8_PATHS} ${V8_PATHS_DEBUG}
	PATH_SUFFIXES ${V8_LIBRARY_PATH_SUFFIXES}
	DOC "Google V8 JavaScript Engine Library Platform (Debug)"
)

if(CMAKE_BUILD_TYPE EQUAL "Debug" AND V8_LIBRARY_DEBUG AND V8_PLATFORM_LIBRARY_DEBUG)
    set(V8_LIBRARY ${V8_LIBRARY_DEBUG} ${V8_PLATFORM_LIBRARY_DEBUG}) # ${V8_ICU_LIBRARY_DEBUG})
else()
    set(V8_LIBRARY ${V8_LIBRARY_RELEASE} ${V8_PLATFORM_LIBRARY_RELEASE}) # ${V8_ICU_LIBRARY_RELEASE})
endif()

set(V8_LIBRARIES ${V8_LIBRARY})

message(STATUS "V8_LIBRARIES: ${V8_LIBRARIES}")


# List of libraries linked for LIBJS
set(LIBJS_LINK
    value_description
    ${V8_LIBRARIES}
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
