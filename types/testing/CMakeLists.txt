# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

set(TYPES_TESTING_EXTRA_LIBS
    types
    arch
    value_description
    base
)

add_mldb_test(date_test "${TYPES_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(localdate_test "${TYPES_TESTING_EXTRA_LIBS}" "boost;valgrind")
add_mldb_test(string_test "${TYPES_TESTING_EXTRA_LIBS};Boost::regex;ICU::i18n" "boost")
set_compile_options(string_test.cc "-I${ICU_INCLUDE_DIRS}")
add_mldb_test(json_handling_test "${TYPES_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(value_description_test "${TYPES_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(periodic_utils_test "${TYPES_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(reader_test "${TYPES_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(json_parsing_test "${TYPES_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(any_test "any;${TYPES_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(decode_uri_test "${TYPES_TESTING_EXTRA_LIBS}" "boost")