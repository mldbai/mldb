# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

find_package(Python REQUIRED COMPONENTS Interpreter Development)

# Define the sources for the Python interpreter
set(PYTHON_INTERPRETER_SOURCES
    python_interpreter.cc
    capture_stream.cc
)

# Define the libraries to link with the Python interpreter
set(PYTHON_INTERPRETER_LINK
    ${Python_LIBRARIES}
    Boost::python312
    vfs
    arch
    value_description
    types
    utils
    runner
)

# Setting compile options for the Python interpreter
set_source_files_properties(${PYTHON_INTERPRETER_SOURCES} PROPERTIES INCLUDE_DIRECTORIES "${Python_INCLUDE_DIRS}")

# Create the Python interpreter library
add_library(python_interpreter ${PYTHON_INTERPRETER_SOURCES})
target_link_libraries(python_interpreter PRIVATE ${PYTHON_INTERPRETER_LINK})

# Define the sources for the Python plugin
set(PYTHON_PLUGIN_SOURCES
    python_plugin.cc
    python_plugin_context.cc
    python_entities.cc
    python_converters.cc
)

# Define the libraries to link with the Python plugin
set(PYTHON_PLUGIN_LINK
    value_description
    ${Python_LIBRARIES}
    Boost::python312
    mldb_core
    mldb_builtin_base
    python_interpreter
    arch
    utils
    types
    mldb_engine
    log
    any
    rest
    rest_entity
    json_diff
    logging
    vfs
    base
    sql_types
    http
)

# Setting compile options for the Python plugin
set_source_files_properties(${PYTHON_PLUGIN_SOURCES} PROPERTIES INCLUDE_DIRECTORIES "${Python_INCLUDE_DIRS}")

# Create the Python plugin library
add_library(mldb_python_plugin ${PYTHON_PLUGIN_SOURCES})
target_link_libraries(mldb_python_plugin PRIVATE ${PYTHON_PLUGIN_LINK})

# Include subdirectories for further build instructions
#add_subdirectory(module) # TODO: figure out how to make CMake know about installing of Python stuff
add_subdirectory(testing)
