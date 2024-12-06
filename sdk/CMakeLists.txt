# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# We extract header files for the development of external plugins, until such
# time as we have a fully separate API.  We start with a set of headers we
# want to include, and we add to those a list of dependency headers that are
# required to include them.
#
# We do this by running GCC in dependency mode on the header itself, and then
# looking at the files that are included by the header, and capturing the
# output.

if(NOT PREMAKE)

# input: header_deps_onefile=$(shell $(CXX) $(CXXFLAGS) -MM -o - $(1) | tr ' ' '\n' | tr -d '\\' | sort | uniq | grep -v sdk_include_seed | grep -v ':' | grep -v '^/' | sed -e 's!^mldb/mldb/!!' -e 's!^mldb/!!')

# Define a function to process headers
function(process_headers header output_var)
    execute_process(
        COMMAND ${CMAKE_CXX_COMPILER} ${CMAKE_CXX_FLAGS} -M ${header}
        COMMAND tr " " "\\n"
        COMMAND tr -d "\\\\"
        COMMAND sort
        COMMAND uniq
        COMMAND grep -v sdk_include_seed
        COMMAND grep -v ":"
        COMMAND grep -v "^/"
        COMMAND sed "s!^mldb/mldb/!!" "s!^mldb/!!"
        OUTPUT_VARIABLE ${output_var}
    )
endfunction()

# input: $(INC)/mldb/%.h: ./mldb/%.h
add_custom_target(mldb_headers ALL
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/mldb
    COMMAND ${CMAKE_COMMAND} -E copy ./mldb/$*.h ${CMAKE_CURRENT_BINARY_DIR}/mldb/$*.h
    DEPENDS ./mldb/$*.h
)

set(MLDB_SDK_INCLUDE_SEED "mldb/sdk/sdk_include_seed.cc")
process_headers(${MLDB_SDK_INCLUDE_SEED} MLDB_ALL_HEADERS)

add_custom_target(mldb_dev_headers DEPENDS ${MLDB_ALL_HEADERS})
add_dependencies(mldb_dev_headers mldb_headers)

# input: expand_wildcards=$(shell find $(1) -type f)
function(expand_wildcards directory output_var)
    file(GLOB_RECURSE ${output_var} "${directory}/*")
endfunction()

# The docker related directives are highly specific to Makefile-based workflows
# and typically don't have a direct equivalent in CMake. The equivalent functionality
# needs to be handled by the developer through a custom command or script.

endif()
