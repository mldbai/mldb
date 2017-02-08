// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* MLDB-896_table_expression_serialised_as_string.cc
   Francois Maillet, 14 septembre 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   Check that we're able to start from a JSON, put it into an Any, then
   convert it to JSON and back again to its structured type.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <string>
#include <vector>
#include <boost/test/unit_test.hpp>
#include "arch/exception.h"
#include "jml/utils/string_functions.h"
#include "jml/utils/vector_utils.h"
#include "plugins/accuracy.h"
#include "mldb/types/value_description.h"


using namespace std;

using namespace MLDB;


BOOST_AUTO_TEST_CASE( tbl_expression_serialised_as_string )
{
    // create config
    string jsConf = "{\"testingData\": { \"from\": {\"id\": \"patate\"},   \
                                        \"select\": \"{*} as features, CLICK IS NOT NULL as label, SCORE as score\" }, \
                      \"outputDataset\": {\"type\": \"beh.mutable\", \"id\":\"output_tbl\"} \
                     }";

    Json::Value conf;
    Json::Reader reader;
    if (!reader.parse(jsConf, conf))
        throw MLDB::Exception("can't parse js conf");
    
    // convert it to an Any, the way it would be received in
    // the params field of a procedure config struct
    Any params(conf);
    auto classifierConfig = params.convert<AccuracyConfig>();

    // send it to json
    auto nowInJson = jsonEncode<AccuracyConfig>(classifierConfig);

    cout << nowInJson.toStyledString() << endl;
    cout << " --- " << endl;

    // convert it back to it's structured form. this should not fail
    BOOST_CHECK_NO_THROW(jsonDecode<AccuracyConfig>(nowInJson));


}

