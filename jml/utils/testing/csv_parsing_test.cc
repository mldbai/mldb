// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* fixed_array_test.cc
   Jeremy Barnes, 8 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   
   Test of the fixed array class.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/jml/utils/csv.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/base/parse_context.h"

#include <sstream>
#include <fstream>

using namespace std;
using namespace ML;
using namespace MLDB;

using boost::unit_test::test_suite;

vector<string> parseCsvLine(const std::string & line)
{
    ParseContext context(line, line.c_str(), line.c_str() + line.size());
    return expect_csv_row(context);
}

void testCsvLine(const std::string & line,
                 const std::vector<std::string> & values)
{
    vector<string> parsed = parseCsvLine(line);
    BOOST_CHECK_EQUAL(parsed, values);
}

BOOST_AUTO_TEST_CASE (test1)
{
    testCsvLine("", {});
    testCsvLine(",", {"",""});
    testCsvLine("\"\"", {""});
    testCsvLine("\"\",", {"",""});
    testCsvLine("\"\",\"\"", {"",""});
}
