# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Add the test for python_interpreter_test with necessary libraries and boost framework.
add_mldb_test(python_interpreter_test 
    "python_interpreter;${Python_LIBRARIES};Boost::python312;arch;value_description;types" 
    "boost"
)

set_source_files_properties(python_interpreter_test.cc PROPERTIES INCLUDE_DIRECTORIES "${Python_INCLUDE_DIRS}")
