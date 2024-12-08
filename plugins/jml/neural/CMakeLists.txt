# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Makefile for neural functions
# Jeremy Barnes, 1 April 2006
# Copyright (c) 2006 Jeremy Barnes.  All rights reserved.

set(LIBNEURAL_SOURCES
    perceptron.cc
    perceptron_defs.cc
    layer.cc
    dense_layer.cc
    perceptron_generator.cc
    parameters.cc
    transfer_function.cc
    layer_stack.cc
    discriminative_trainer.cc
    auto_encoder.cc
    auto_encoder_stack.cc
    twoway_layer.cc
    auto_encoder_trainer.cc
    reverse_layer_adaptor.cc
    reconstruct_layer_adaptor.cc
    output_encoder.cc
)

add_library(neural ${LIBNEURAL_SOURCES})

target_link_libraries(neural PRIVATE utils db algebra arch judy boosting stats)

if(CUDA_ENABLED)
    set(LIBNEURAL_CUDA_SOURCES backprop_cuda.cu)

    add_library(neural_cuda ${LIBNEURAL_CUDA_SOURCES})

    target_link_libraries(neural_cuda PRIVATE neural arch_cuda cudart_ocelot)
endif()

add_subdirectory(testing)
