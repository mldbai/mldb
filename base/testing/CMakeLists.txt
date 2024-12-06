# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# Setting environment flag for MLDB (not directly translated into CMake logic)
# export MLDB_NO_TIMING_TESTS=1

add_mldb_test(thread_pool_test "base;arch" "boost;timed")
add_mldb_test(thread_queue_test "base;arch" "boost;timed")
add_mldb_test(per_thread_accumulator_test "base;arch" "boost")