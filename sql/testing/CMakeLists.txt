# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# sql_testing.mk
# Jeremy Barnes, 10 April 2016
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

add_mldb_test(path_test "sql_types;vfs;arch;types;base;value_description" "boost;valgrind")
add_mldb_test(path_order_test "sql_types;vfs;arch;types;base;value_description" "boost")
add_mldb_test(path_benchmark "sql_types;vfs;arch;types;base;value_description" "boost")
add_mldb_test(eval_sql_test "sql_expression;vfs;arch;types;base;value_description" "boost")
