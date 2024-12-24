# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

set(LIBBOOSTING_TOOLS_SOURCES
    boosting_tool_common.cc
    datasets.cc
)

add_library(boosting_tools
    ${LIBBOOSTING_TOOLS_SOURCES}
)

target_link_libraries(boosting_tools
    utils
    db
    arch
    boosting
    base
    vfs
)

add_executable(classifier_training_tool classifier_training_tool.cc)

target_link_libraries(classifier_training_tool
    boosting
    boosting_tools
    utils
    arch
    Boost::program_options
    jml_utils
)

if(CUDA_ENABLED)
    target_link_libraries(classifier_training_tool boosting_cuda)
endif()

add_executable(training_data_tool training_data_tool.cc)

target_link_libraries(training_data_tool
    boosting
    boosting_tools
    utils
    arch
    Boost::program_options
)
