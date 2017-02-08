/** run_json_test_suite.cc
    Jeremy Barnes, 26 October 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.
    
    Run the JSON test suite.
*/

#include "mldb/types/json_parsing.h"
#include "mldb/types/json_printing.h"
#include "mldb/ext/jsoncpp/json.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/arch/exception.h"
#include <sstream>
#include <set>
#include <glob.h>

using namespace std;
using namespace MLDB;

size_t expectedPasses = 0;
size_t unexpectedPasses = 0;
size_t expectedFailures = 0;
size_t unexpectedFailures = 0;
size_t optionalPasses = 0;
size_t optionalFailures = 0;
size_t reserializationErrors = 0;

// These are invalid tests (they say they should be supported but include byte
// order marks which don't have to be supported according to JSON).
std::set<std::string> skippedTests = { "y_string_utf16.json" };

void testFile(const std::string & filename)
{
    std::ostringstream str;
    filter_istream stream(filename);
    
    str << stream.rdbuf();

    auto pos = filename.rfind('/');
    string rest(filename, pos + 1);

    if (skippedTests.count(rest))
        return;

    char test = rest.at(0);
    cerr << "test " << rest << endl;

    Json::Value val, other;

    if (test == 'y') {
        try {
            StringJsonParsingContext context(str.str());
            val = context.expectJson();
            context.expectEof();
            other = Json::parse(str.str());
            ++expectedPasses;
        } catch (const std::exception & exc) {
            cerr << "BAD FAILURE " << exc.what() << endl;
            ++unexpectedFailures;
            cerr << "<<<<<<<<<<<<<<<<<<<" << endl;
            cerr << str.str() << endl;
            cerr << "<<<<<<<<<<<<<<<<<<<" << endl;
        }
    }
    else if (test == 'n') {
        try {
            MLDB_TRACE_EXCEPTIONS(false);
            StringJsonParsingContext context(str.str());
            val = context.expectJson();
            context.expectEof();
            cerr << "SHOULD HAVE FAILED" << endl;
            ++unexpectedPasses;
        } catch (const std::exception & exc) {
            ++expectedFailures;
        }
    }
    else if (test == 'i') {
        try {
            MLDB_TRACE_EXCEPTIONS(false);
            StringJsonParsingContext context(str.str());
            val = context.expectJson();
            context.expectEof();
            cerr << "OPTIONAL PASSED" << endl;
            ++optionalPasses;
        } catch (const std::exception & exc) {
            cerr << "OPTIONAL FAILURE" << endl;
            ++optionalFailures;
        }
    }

    if (!val.isNull()) {
        Utf8String str;
        Utf8StringJsonPrintingContext context(str);
        context.writeJson(val);
        
        StringJsonParsingContext context2(str.rawString());
        auto val2 = context2.expectJson();

        if (val != val2 && val.toString() != val2.toString()) {
            cerr << "Invalid JSON serialization/reserialization" << endl;
            cerr << "val: " << val << endl;
            cerr << "val2: " << val2 << endl;
            ++reserializationErrors;
        }
    }
}

int main(int argc, char ** argv)
{
    auto onFile = [&] (const std::string & uri,
                       const FsObjectInfo & info,
                       const OpenUriObject & open,
                       int depth)
        -> bool
        {
            testFile(uri);
            return true;
        };
    
    forEachUriObject("mldb/ext/JSONTestSuite/test_parsing",
                     onFile);

    cerr << "y tests: passed " << expectedPasses << " failed "
         << unexpectedFailures << endl;
    cerr << "n tests: passed " << unexpectedPasses << " failed "
         << expectedFailures << endl;
    cerr << "i tests: passed " << optionalPasses << " failed "
         << optionalFailures << endl;
    cerr << "reserialization errors: " << reserializationErrors << endl;
    
    return unexpectedFailures || reserializationErrors ? 1 : 0;
}
