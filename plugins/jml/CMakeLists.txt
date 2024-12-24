# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Library for jml_utils
set(LIBJML_UTILS_SOURCES
    configuration.cc
)

set(LIBJML_UTILS_LINK
    utils
    arch
    base
)

add_library(jml_utils ${LIBJML_UTILS_SOURCES})
target_link_libraries(jml_utils ${LIBJML_UTILS_LINK})

# Include sub-makes
add_subdirectory(algebra)
add_subdirectory(stats)
add_subdirectory(tsne)
add_subdirectory(jml)

# Library for ml
set(LIBML_SOURCES
    dense_classifier.cc
    separation_stats.cc
    dense_classifier_scorer.cc
    dense_feature_generator.cc
    data_partition.cc
    scorer.cc
    prediction_accumulator.cc
    bucketing_probabilizer.cc
    distribution_pooler.cc
    kmeans.cc
    em.cc
    value_descriptions.cc
    configuration.cc
)

set(LIBML_LINK
    ${STD_FILESYSTEM_LIBNAME}
    types
    value_description
    algebra
    boosting
    arch
    base
    utils
    db
    vfs
    block
)

add_library(ml ${LIBML_SOURCES})
target_link_libraries(ml ${LIBML_LINK})

# Include sub-make for ml_testing
add_subdirectory(testing)

find_library(SVM_LIBRARY svm)

# Jml plugins
set(LIBMLDB_JML_PLUGIN_SOURCES
    randomforest_procedure.cc
    classifier.cc
    probabilizer.cc
    svm.cc
    jml_plugin.cc
    accuracy.cc
    experiment_procedure.cc
    randomforest.cc
    dataset_feature_space.cc
    kmeans_interface.cc
    em_interface.cc
    tsne_interface.cc
)

set(LIBMLDB_JML_PLUGIN_LINK
    ml
    boosting
    mldb_core
    mldb_engine
    arch
    mldb_core
    types
    utils
    sql_expression
    value_description
    base
    progress
    rest
    db
    vfs
    log
    tsne
    link
    svm
    rest
    any
    watch
    rest_entity
    jml_utils
    mldb_builtin_base
    mldb_builtin
    sql_types
    algebra
    block
)

add_library(mldb_jml_plugin ${LIBMLDB_JML_PLUGIN_SOURCES})
target_link_libraries(mldb_jml_plugin ${LIBMLDB_JML_PLUGIN_LINK})

# Uncomment and handle additional configuration if needed:
# target_include_directories(mldb_jml_plugin PRIVATE ${CMAKE_SOURCE_DIR}/mldb/jml/ext)

# Additional plugin configurations can be defined or uncommented here
# Custom CMake functions or macros similar to `mldb_plugin_library` or `mldb_builtin_plugin` can be implemented.
