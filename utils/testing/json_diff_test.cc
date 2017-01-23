// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* json_diff_test.cc
   Jeremy Barnes, 2 November 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   Test of JSON diffs.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/utils/json_diff.h"
#include "mldb/utils/json_utils.h"
#include "mldb/vfs/filter_streams.h"

using namespace std;
using namespace MLDB;

namespace MLDB {

// jsonDecode implementation for any type which:
// 1) has a default description;
template<typename T>
T jsonDecodeFile(const std::string & filename, T * = 0)
{
    T result;
    
    filter_istream stream(filename);
    
    static auto desc = getDefaultDescriptionSharedT<T>();
    StreamingJsonParsingContext context(filename, stream);
    desc->parseJson(&result, context);
    return result;
}

} // namespace MLDB

#if 1
BOOST_AUTO_TEST_CASE( test_diff_1 )
{
    Json::Value v1;
    Json::Value v2;

    auto diff = jsonDiff(v1, v2);

    cerr << jsonEncode(diff);

    BOOST_CHECK(diff.empty());

    v1["new element"] = 10;

    diff = jsonDiff(v1, v2);

    cerr << jsonEncode(diff);

    BOOST_CHECK(!diff.empty());

    v2["new element"] = 20;

    diff = jsonDiff(v1, v2);

    cerr << jsonEncode(diff);
}
#endif

#if 0

BOOST_AUTO_TEST_CASE(test_array_diff1)
{
    Json::Value arr1 = { 0 };
    Json::Value arr2 = { 1 };

    auto diff = jsonDiff(arr1, arr2);

    cerr << jsonEncode(diff);

    BOOST_REQUIRE(!diff.empty());
    BOOST_REQUIRE(diff.elements[0].oldValue);
    BOOST_REQUIRE(diff.elements[0].newValue);
    BOOST_CHECK_EQUAL(*diff.elements[0].oldValue, 0);
    BOOST_CHECK_EQUAL(*diff.elements[0].newValue, 1);
}

BOOST_AUTO_TEST_CASE(test_array_diff2)
{
    Json::Value arr1 = { 0 };
    Json::Value arr2 = { 0, 1 };

    auto diff = jsonDiff(arr1, arr2);

    cerr << jsonEncode(diff);

    BOOST_REQUIRE_EQUAL(diff.elements.size(), 1);
    BOOST_REQUIRE(diff.elements[0].newValue);
    BOOST_CHECK_EQUAL(*diff.elements[0].newValue, 1);
}

BOOST_AUTO_TEST_CASE(test_array_diff3)
{
    Json::Value arr1 = { 0, 1 };
    Json::Value arr2 = { 0 };

    auto diff = jsonDiff(arr1, arr2);

    cerr << jsonEncode(diff);

    BOOST_REQUIRE_EQUAL(diff.elements.size(), 1);
    BOOST_REQUIRE(diff.elements[0].oldValue);
    BOOST_CHECK_EQUAL(*diff.elements[0].oldValue, 1);
}

BOOST_AUTO_TEST_CASE(test_array_diff4)
{
    Json::Value arr1 = { 0, 1 };
    Json::Value arr2 = { 1 };

    auto diff = jsonDiff(arr1, arr2);

    cerr << jsonEncode(diff);

    BOOST_REQUIRE_EQUAL(diff.elements.size(), 1);
    BOOST_REQUIRE(diff.elements[0].oldValue);
    BOOST_CHECK_EQUAL(*diff.elements[0].oldValue, 0);
}

BOOST_AUTO_TEST_CASE(test_array_diff5)
{
    Json::Value arr1 = { 1 };
    Json::Value arr2 = { 0, 1 };

    auto diff = jsonDiff(arr1, arr2);

    cerr << jsonEncode(diff);

    BOOST_CHECK_EQUAL(diff.elements.size(), 1);
    BOOST_REQUIRE(diff.elements[0].newValue);
    BOOST_CHECK_EQUAL(*diff.elements[0].newValue, 0);
}

BOOST_AUTO_TEST_CASE(test_array_diff6)
{
    Json::Value arr1 = { 2, 3 };
    Json::Value arr2 = { 1, 6, 3 };

    auto diff = jsonDiff(arr1, arr2);

    cerr << jsonEncode(diff);

    BOOST_CHECK(!diff.empty());
    BOOST_CHECK_EQUAL(diff.elements.size(), 2);
    //BOOST_CHECK_EQUAL(*diff.elements.begin()->second.newValue, 0);
}

#endif

#if 1
void test_patch_diff(const Json::Value & val1,
                     const Json::Value & val2)
{
    cerr << endl << "---- new test" << endl;
    cerr << "val1 = " << val1;
    cerr << "val2 = " << val2;
    auto diff = jsonDiff(val1, val2);
    
    Json::Value patched;
    JsonDiff conflicts;

    std::tie(patched, conflicts)
        = jsonPatch(val1, diff);

    BOOST_CHECK(!conflicts);

    if (patched != val2 || conflicts) {
        cerr << "----- patch diff error" << endl;
        cerr << "val1 = " << val1
             << "val2 = " << val2
             << "diff = " << diff
             << "patched = " << patched
             << "conflicts = " << conflicts
             << endl;
    }

    BOOST_CHECK_EQUAL(patched, val2);
}

BOOST_AUTO_TEST_CASE(test_patch_and_diff)
{
    Json::Value v1;
    Json::Value v2("hello");
    Json::Value v3(3);
    Json::Value o1;
    o1["one"] = v1;
    o1["two"] = v2;

    Json::Value o2 = o1;
    o2["three"] = v3;

    Json::Value o4;
    o4["x"] = o1;
    o4["y"] = o2;

    Json::Value o5;
    o5["x"] = o1;
    o5["y"] = o4;

    test_patch_diff(v1, v1);
    test_patch_diff(v1, v2);
    test_patch_diff(v1, v3);
    test_patch_diff(v2, v3);
    test_patch_diff(o1, v1);
    test_patch_diff(o1, o2);
    test_patch_diff(o2, o1);
    test_patch_diff(o4, o5);
    test_patch_diff(o5, o4);
}


#if 1
BOOST_AUTO_TEST_CASE(test_array_diff_big)
{
    Json::Value fixtures
        = jsonDecodeFile<Json::Value>("mldb/utils/testing/json_diff_fixtures.json", (Json::Value *)0);

    auto diff = jsonDiff(fixtures["test1"]["val1"],
                         fixtures["test1"]["val2"]);

    //cerr << jsonEncode(diff);
}
#endif

void test_patch_diff2(const Json::Value & v1, const Json::Value & v2)
{
    test_patch_diff(v1, v2);
    test_patch_diff(v2, v1);
}

BOOST_AUTO_TEST_CASE(test_array_patch)
{
    Json::Value v1 = { 1, 2 };
    Json::Value v2 = { 1, 2, 3 };
    Json::Value v3 = { 2, 3 };
    Json::Value v4 = { 1, 6, 3 };
    Json::Value v5 = Json::Value(Json::arrayValue);
    Json::Value v6 = { 1, 6, 6, 6, 6, 6, 6, 3 };
    Json::Value v7 = { 1, 6, 6, 6, 2, 6, 6, 3 };
    Json::Value v8 = { 1, 6, 6, 6, 2, 5, 5, 3 };
    Json::Value v9 = { 3, 2, 1 };
    Json::Value v10 = { 3, 2, 1 };
    v10[1] = "hello";
    Json::Value v11 = v10;
    v11[1] = Json::Value();

    vector<Json::Value> vals = {
        v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11
    };

    for (unsigned i = 0;  i < vals.size();  ++i) {
        for (unsigned j = 0;  j <= i;  ++j) {
            test_patch_diff2(vals[i], vals[j]);
        }
    }

#if 0
    test_patch_diff(v1, v2);
    test_patch_diff(v1, v3);
    test_patch_diff(v1, v4);
    test_patch_diff(v2, v3);

    auto diff = jsonDiff(v1, v2);

    cerr << "diff = " << diff << endl;

    Json::Value v3;
    JsonDiff conflicts;
    std::tie(v3, conflicts) = jsonPatch(v1, diff);

    BOOST_CHECK_EQUAL(v3, v2);


    diff = jsonDiff(v2, v1);

    std::tie(v3, conflicts) = jsonPatch(v2, diff);

    BOOST_CHECK_EQUAL(v3, v1);
#endif
}


BOOST_AUTO_TEST_CASE(test_summarize)
{
    Json::Value fixtures
        = jsonDecodeFile<Json::Value>("mldb/utils/testing/json_diff_fixtures.json", (Json::Value *)0);

    cerr << jsonPrintAbbreviated(fixtures["test1"]["val1"], 100) << endl;
}

#if 0
BOOST_AUTO_TEST_CASE(test_summarize2)
{
    Json::Value v
        = jsonDecodeFile<Json::Value>("failure-v.json", (Json::Value *)0);

    cerr << jsonPrintAbbreviated(v, 30) << endl;
}
#endif

#if 1
BOOST_AUTO_TEST_CASE( test_json_patch )
{
    int cnt(1);
    auto testArrayPatching = [&] (const Json::Value & a,
                                  const Json::Value & b) {
        cerr << cnt << " - testing with a = "
             << a.toStringNoNewLine()
             << " and b = "
             << b.toStringNoNewLine()
             << endl;
        cnt++;

        auto edits = jsonDiff(a, b);
        // cerr << "  diff = " << edits << endl;

        // cerr << "patching...\n";
        Json::Value patched;
        JsonDiff conflicts;

        std::tie(patched, conflicts) = jsonPatch(a, edits);

        // cerr << "  conflicts: " << conflicts << "\n";
        BOOST_CHECK(!conflicts);
        BOOST_CHECK_EQUAL(patched, b);
    };

    /* value replacement */
    testArrayPatching({1, 2, 3}, {1, 2, 4});
    testArrayPatching({1, 2, 3}, {1, 8, 3});
    testArrayPatching({1, 2, 3}, {9, 2, 3});
    testArrayPatching({1, 2, 3}, {1, 3, 3});
    testArrayPatching({1, 2, 3}, {2, 2, 3});
    testArrayPatching({1, 2, 3}, {1, 1, 3});

    /* value insertion */
    testArrayPatching({1, 2, 3}, {1, 2, 3, 4});
    testArrayPatching({1, 2, 3}, {1, 1, 2, 3});
    testArrayPatching({1, 2, 3}, {1, 2, 2, 3});
    testArrayPatching({1, 2, 3}, {1, 2, 3, 3});
    testArrayPatching({1, 2, 3}, {1, 1, 2, 2, 3, 3, 4});

    /* value removal */
    testArrayPatching({1, 2, 3}, {2, 3});
    testArrayPatching({1, 2, 3}, {1, 3});
    testArrayPatching({1, 2, 3}, {1, 2});
    testArrayPatching({9, 13, 7}, {13});
    testArrayPatching({1, 1, 2, 2, 3, 3, 4}, {1, 2, 3});

    {
        /* ensure that a conflict is reported for addition/removal cases */
        Json::Value oldA{1, 2, 3, 4};
        Json::Value oldB{1, 2, 3};
        Json::Value recentA{1, 2, 3, 5};

        cerr << cnt << " - testing with a = "
             << oldA.toStringNoNewLine()
             << " and b = "
             << oldB.toStringNoNewLine()
             << " applied to : "
             << recentA.toStringNoNewLine()
             << endl;

        auto edits = jsonDiff(oldA, oldB);

        Json::Value patched;
        JsonDiff conflicts;
        std::tie(patched, conflicts) = jsonPatch(recentA, edits);

        BOOST_CHECK((bool) conflicts);
        BOOST_CHECK_NE(patched, oldB);
    }
}
#endif
#endif

BOOST_AUTO_TEST_CASE( test_non_strict )
{
    Json::Value val1(0);
    Json::Value val2(0.0);
    Json::Value val3(0U);

    BOOST_CHECK(jsonDiff(val1, val2, true /* strict */));
    BOOST_CHECK(jsonDiff(val1, val3, true /* strict */));
    BOOST_CHECK(jsonDiff(val2, val3, true /* strict */));
    BOOST_CHECK(!jsonDiff(val1, val2, false /* strict */));
    BOOST_CHECK(!jsonDiff(val1, val3, false /* strict */));
    BOOST_CHECK(!jsonDiff(val2, val3, false /* strict */));

    Json::Value val4, val5;
    val4["x"] = val1;
    val5["x"] = val2;

    BOOST_CHECK(jsonDiff(val4, val5, true /* strict */));
    BOOST_CHECK(!jsonDiff(val4, val5, false /* strict */));
}
