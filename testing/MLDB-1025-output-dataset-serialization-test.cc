/* MLDB-1025-output-dataset-serialization-test.cc
   Guy Dumais, 4 November 2015
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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

using namespace MLDB;


BOOST_AUTO_TEST_CASE( test_output_dataset_parsing )
{
    // create config
    string jsConf = "{\"inputData\": { \"from\" : {\"id\": \"patate\"}}, \
                      \"outputDataset\": \"plum\"}";

    Json::Value conf;
    Json::Reader reader;
    if (!reader.parse(jsConf, conf))
        throw MLDB::Exception("can't parse js conf");

    // convert it to an Any, the way it would be received in
    // the params field of a procedure config struct
    Any params(conf);
    auto transformConfig = params.convert<TransformDatasetConfig>();

    BOOST_CHECK_EQUAL(transformConfig.outputDataset.id, "plum");
}

template <typename ConfigType> 
ConfigType createConfig(const std::string & datasetName, const std::string inputDatasetKey)
{
    string jsConf = "{\""+inputDatasetKey+"\": { \"from\" : {\"id\": \"pwel\"}}, \
                        \"" + datasetName + "\" : {\"id\": \"patate\"}}";
    Json::Value conf;
    Json::Reader reader;
    if (!reader.parse(jsConf, conf))
        throw MLDB::Exception("can't parse js conf");
    
    Any params(conf);
    return params.convert<ConfigType>();
}

BOOST_AUTO_TEST_CASE( test_output_dataset_defaults )
{
    std::vector<std::pair<PolyConfigT<Dataset>, std::string> > expectedDatasetType = {
        { createConfig<BucketizeProcedureConfig>("outputDataset", "inputData").outputDataset, "sparse.mutable" },
        { createConfig<StatsTableProcedureConfig>("outputDataset", "trainingData").output, "sparse.mutable" },
        { createConfig<TransformDatasetConfig>("outputDataset", "inputData").outputDataset, "sparse.mutable" },
        { createConfig<TsneConfig>("rowOutputDataset", "trainingData").output, "embedding" }
    };
    
    for(auto it : expectedDatasetType) {
        BOOST_CHECK_EQUAL(it.first.id, "patate");
        BOOST_CHECK_EQUAL(it.first.type, it.second);
    }
}
