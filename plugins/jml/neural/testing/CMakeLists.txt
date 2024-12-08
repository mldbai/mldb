# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Makefile for neural network testing
# Jeremy Barnes, 2 November 2009
# Copyright (c) 2009 Jeremy Barnes.  All rights reserved.

# Convert Makefile tests to CMake tests using add_mldb_test

add_mldb_test(dense_layer_test "neural;utils;arch;db" "boost")
add_mldb_test(layer_stack_test "neural;utils;arch;db" "boost")
add_mldb_test(discriminative_trainer_test "neural" "boost")
add_mldb_test(twoway_layer_test "neural;utils;arch;db" "boost;manual")
add_mldb_test(perceptron_test "neural;utils;boosting" "boost;manual")
add_mldb_test(output_encoder_test "neural" "boost")
