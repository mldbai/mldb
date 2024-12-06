# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Test case for filter_streams_test
add_mldb_test(filter_streams_test 
  "vfs;filesystem;boost_system;value_description;arch;types"
  "boost"
)

# Test dependencies for filter_streams_test
set_target_properties(filter_streams_test PROPERTIES EXCLUDE_FROM_ALL TRUE)
add_dependencies(filter_streams_test lz4cli zstd)

# Test case for compressor_test
add_mldb_test(compressor_test 
  "vfs;filesystem;boost_system;value_description;arch;types"
  "boost"
)
