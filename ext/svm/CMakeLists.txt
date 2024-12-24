# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Creates a library from svm.cpp with no additional linked libraries
add_library(svm svm.cpp)
set_source_files_properties(svm.cpp PROPERTIES COMPILE_OPTIONS "-Wno-deprecated")
