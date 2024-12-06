# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

#input: $(eval $(call test,fs_utils_test,vfs_handlers test_utils arch value_description types vfs,boost manual))
add_mldb_test(fs_utils_test "vfs_handlers;test_utils;arch;value_description;types;vfs" "boost;manual")
