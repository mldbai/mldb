# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Define the sources for the Python addon
set(MLDB_PYTHON_ADDON_SOURCES 
    find_mldb_environment.cc)

# Define the libraries required for linking
set(MLDB_PYTHON_ADDON_LINK 
    ${PYTHON_LIBRARY}
    nanobind
    mldb_python_plugin
    arch)

# Call the python_addon function with the specified sources and link libraries
python_addon(_mldb "${MLDB_PYTHON_ADDON_SOURCES}" "${MLDB_PYTHON_ADDON_LINK}")

# Set the compile options for the Python plugin sources
set_compile_options(${PYTHON_PLUGIN_SOURCES} "-I${PYTHON_INCLUDE_PATH}")

# Generate python module
python_module(mldb ${notdir_glob_CWD("*.py")} _mldb)

# Ensure mldb_runner depends on the mldb Python module
add_dependencies(${BIN}/mldb_runner ${PYTHON_mldb_DEPS})
