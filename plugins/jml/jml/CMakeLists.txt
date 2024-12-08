# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# CMake equivalent for the boosting functions Makefile

set(LIBBOOSTING_SOURCES
    boosted_stumps.cc
    classifier.cc
    data_aliases.cc
    decoded_classifier.cc
    decision_tree.cc
    null_feature_space.cc
    decoder.cc
    dense_features.cc
    evaluation.cc
    feature_info.cc
    feature_set.cc
    feature_space.cc
    glz_classifier.cc
    naive_bayes.cc
    null_classifier.cc
    null_decoder.cc
    probabilizer.cc
    sparse_features.cc
    stump.cc
    training_data.cc
    training_index.cc
    training_index_entry.cc
    weighted_training.cc
    transformed_classifier.cc
    stump_training.cc
    config_options.cc
    stump_regress.cc
    boosted_stumps_generator.cc
    bagging_generator.cc
    boosting_generator.cc
    naive_bayes_generator.cc
    decision_tree_generator.cc
    feature_transformer.cc
    glz_classifier_generator.cc
    classifier_generator.cc
    stump_generator.cc
    binary_symmetric.cc
    early_stopping_generator.cc
    stump_training_bin.cc
    feature_transform.cc
    transform_list.cc
    committee.cc
    boosting_training.cc
    null_classifier_generator.cc
    fasttext_classifier.cc
    fasttext_generator.cc
    onevsall.cc
    onevsall_generator.cc
    tree.cc
    split.cc
    training_index_iterators.cc
    feature.cc
    bit_compressed_index.cc
    label.cc
    info_override_feature_space.cc
    multilabel_training_data.cc
)

set(LIBBOOSTING_LINK
    jml_utils utils db algebra arch judy fasttext log arch base value_description Boost::timer vfs
)

add_library(boosting ${LIBBOOSTING_SOURCES})
target_link_libraries(boosting ${LIBBOOSTING_LINK})

if(CUDA_ENABLED)
    set(LIBBOOSTING_CUDA_SOURCES
        stump_training_cuda.cu
        stump_training_cuda_host.cc
        backprop_cuda.cu
    )

    set(LIBBOOSTING_CUDA_LINK
        boosting arch_cuda cudart_ocelot
    )

    add_library(boosting_cuda ${LIBBOOSTING_CUDA_SOURCES})
    target_link_libraries(boosting_cuda ${LIBBOOSTING_CUDA_LINK})
endif()

add_subdirectory(tools)
add_subdirectory(testing)
