// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* MLDB-991_permutator_permutations 
   Francois Maillet, 17 septembre 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   Check that the permutator permutations are working
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <string>
#include <vector>
#include <boost/test/unit_test.hpp>
#include "arch/exception.h"
#include "jml/utils/string_functions.h"
#include "jml/utils/vector_utils.h"
#include "plugins/permuter_procedure.h"

using namespace std;

using namespace MLDB;

Json::Value parse(const string & js)
{
    Json::Value root;
    Json::Reader reader;
    if(!reader.parse(js, root))
        throw MLDB::Exception("unable to parse");
    return root;
}

BOOST_AUTO_TEST_CASE( simple_permutation )
{
    string baseConfig = R"foo(
    {
        "args": {
            "hoho" : 5,
            "bouya": "x"
        }
    }    
    )foo";

    string permutations = R"foo(
    {
        "args": {
            "hoho": [1, 2],
            "bouya": ["x", "y"]
        }
    }
    )foo";

    vector<Json::Value> configs;
    auto forEach = [&] (const Json::Value & conf)
        {
            configs.push_back(conf);
        };

    PermutationProcedure::forEachPermutation(parse(baseConfig), parse(permutations), forEach);

    BOOST_CHECK_EQUAL(configs.size(), 4);

    for(auto x: configs)
        cout << x.toStyledString() << endl;


    set<string> keyVals;
    for(auto & conf : configs) {
        keyVals.insert(MLDB::format("%d%s", conf["args"]["hoho"].asInt(),
                                          conf["args"]["bouya"].asString()));
    }

    set<string> validKeyVals = { "1x", "1y", "2x", "2y" };

    for(auto x : keyVals)
    cout << x << endl;
    cout << " -- " << endl;
    for(auto y : validKeyVals)
    cout << y << endl;

    BOOST_CHECK_EQUAL_COLLECTIONS(keyVals.begin(), keyVals.end(),
                                  validKeyVals.begin(), validKeyVals.end());
}


BOOST_AUTO_TEST_CASE( deeper_permutation )
{
    string baseConfig = R"foo(
{
    "args": {
        "hoho" : 5,
        "bouya": "x"
    }
}    
)foo";

    string permutations = R"foo(
{
    "args": {
        "hoho": [1, 2],
        "bouya": ["x", "y"],
        "deep": {
            "learning": ["a", "b", "c"],
            "deeper": {
                "cobb": ["r", "s"]
            }
        }
    }
}
)foo";

    vector<Json::Value> configs;
    auto forEach = [&] (const Json::Value & conf)
        {
            configs.push_back(conf);
        };

    PermutationProcedure::forEachPermutation(parse(baseConfig), parse(permutations), forEach);

    // do we have the right amount of permutations?
    BOOST_CHECK_EQUAL(configs.size(), 2 * 2 * 3 * 2);

//     for(auto x: configs)
//         cout << x.toStyledString() << endl;
// 
    
    set<string> keyvals;
    for(auto & conf : configs) {
        keyvals.insert(MLDB::format("%d%s%s%s", conf["args"]["hoho"].asInt(),
                                              conf["args"]["bouya"].asString(),
                                              conf["args"]["deep"]["learning"].asString(),
                                              conf["args"]["deep"]["deeper"]["cobb"].asString()));
    }


    // do a couple of spot checks. if we have the right amount and the following
    // tests pass, we also have the right structure so we should be fine
    BOOST_CHECK( keyvals.find("1xar") != keyvals.end() );
    BOOST_CHECK( keyvals.find("2xcs") != keyvals.end() );
}
