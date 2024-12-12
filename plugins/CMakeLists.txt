# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# These are in the process of being packaged as full plugins
add_subdirectory(behavior)
add_subdirectory(tabular)
add_subdirectory(jml)
add_subdirectory(nlp)
add_subdirectory(feature_gen)
add_subdirectory(textual)
add_subdirectory(msoffice)
add_subdirectory(git)
add_subdirectory(sqlite)
add_subdirectory(sparse)
add_subdirectory(embedding)

# These have already been packaged as full plugins
# Includes the MLDB plugins for 'html' and 'pro'
#add_subdirectory(html)
#add_subdirectory(pro)

#if(LINUX)
#  add_subdirectory(av)
#endif()
# add_subdirectory(tensorflow) # TensorFlow plugin not included

# No source code, this is just a library to bring everything together
set(LIBMLDB_BUILTIN_PLUGIN_SOURCES mldb_builtin_plugins.cc)

# Needed so that Python plugin can find its header
set_source_files_properties(python_plugin_loader.cc PROPERTIES COMPILE_FLAGS "-I${PYTHON_INCLUDE_PATH}")

# Libraries to link against for MLDB built-in plugins
set(LIBMLDB_BUILTIN_PLUGIN_LINK
    mldb_tabular_plugin
    mldb_behavior_plugin
    mldb_jml_plugin
    mldb_nlp_plugin
    mldb_feature_gen_plugin
    mldb_textual_plugin
    mldb_msoffice_plugin
    mldb_git_plugin
    mldb_sqlite_plugin
    mldb_sparse_plugin
    mldb_embedding_plugin
    sqlite-mldb
    ml
    tsne
    svm
    libstemmer
    edlib
    algebra
    svdlibc
)

# Define the library for built-in plugins
add_library(mldb_builtin_plugins ${LIBMLDB_BUILTIN_PLUGIN_SOURCES})
target_link_libraries(mldb_builtin_plugins ${LIBMLDB_BUILTIN_PLUGIN_LINK})
