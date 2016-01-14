// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* MLDB-1025-output-dataset-serialization-test.cc
   Guy Dumais, 4 November 2015
   Copyright (c) 2015 Datacratic Inc.  All rights reserved.
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
#include "plugins/sql_functions.h"
#include "plugins/bucketize_procedure.h"
#include "plugins/stats_table_procedure.h"
#include "plugins/kmeans.h"
#include "plugins/tsne.h"
#include "mldb/core/procedure.h"
#include "mldb/core/dataset.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/optional_description.h"


using namespace std;
using namespace Datacratic;
using namespace MLDB;


BOOST_AUTO_TEST_CASE( test_output_dataset_parsing )
{
    // create config
    string jsConf = "{\"inputData\": { \"from\" : {\"id\": \"patate\"}}, \
           \"outputDataset\": \"plum\"}";
    
    Json::Value conf;
    Json::Reader reader;
    if (!reader.parse(jsConf, conf))
        throw ML::Exception("can't parse js conf");
    
    // convert it to an Any, the way it would be received in
    // the params field of a procedure config struct
    Any params(conf);
    auto transformConfig = params.convert<TransformDatasetConfig>();

    BOOST_CHECK_EQUAL(transformConfig.outputDataset.id, "plum");
}

template <typename ConfigType> 
ConfigType createConfig(const std::string & datasetName)
{
    string jsConf = "{\"" + datasetName + "\" : {\"id\": \"patate\"}}";
    Json::Value conf;
    Json::Reader reader;
    if (!reader.parse(jsConf, conf))
        throw ML::Exception("can't parse js conf");
    
    Any params(conf);
    return params.convert<ConfigType>();
}

BOOST_AUTO_TEST_CASE( test_output_dataset_defaults )
{
    std::vector<std::pair<PolyConfigT<Dataset>, std::string> > expectedDatasetType = {
        { createConfig<BucketizeProcedureConfig>("outputDataset").outputDataset, "sparse.mutable" },
        { createConfig<StatsTableProcedureConfig>("outputDataset").output, "sparse.mutable" },
        { createConfig<TransformDatasetConfig>("outputDataset").outputDataset, "sparse.mutable" },
        { createConfig<TsneConfig>("rowOutputDataset").output, "embedding" }
    };
    
    for(auto it : expectedDatasetType) {
        BOOST_CHECK_EQUAL(it.first.id, "patate");
        BOOST_CHECK_EQUAL(it.first.type, it.second);
    }
}
