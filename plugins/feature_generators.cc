// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** feature_generators.cc
    Francois Maillet, 27 juillet 2015
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

*/

#include "feature_generators.h"
#include "mldb/server/mldb_server.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/jml/stats/distribution.h"
#include "mldb/jml/utils/guard.h"
#include "mldb/base/parallel.h"
#include "mldb/ml/jml/thread_context.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/ml/value_descriptions.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/types/any_impl.h"


using namespace std;

namespace Datacratic {
namespace MLDB {

/*****************************************************************************/
/* HASHED COLUMN FEAT GEN CONFIG                                             */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(HashedColumnFeatureGeneratorConfig);

HashedColumnFeatureGeneratorConfigDescription::
HashedColumnFeatureGeneratorConfigDescription()
{
    addField("numBits", &HashedColumnFeatureGeneratorConfig::numBits,
             "Number of bits to use for the hash. The number of resulting "
                "buckets will be 2^numBits.", 8);
}

HashedColumnFeatureGenerator::
HashedColumnFeatureGenerator(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
    : BaseT(owner)
{
    functionConfig = config.params.convert<HashedColumnFeatureGeneratorConfig>();

    for(int i=0; i<numBuckets(); i++) {
        outputColumns.emplace_back(ColumnName(ML::format("hashColumn%d", i)),
                                   std::make_shared<Float32ValueInfo>(),
                                   COLUMN_IS_DENSE);
    }
}

HashedColumnFeatureGenerator::
~HashedColumnFeatureGenerator()
{
}

FeatureGeneratorOutput
HashedColumnFeatureGenerator::
call(const FeatureGeneratorInput & input) const
{
    ML::distribution<float> result(numBuckets());

    ML::Lightweight_Hash_Set<uint64_t> doneHashes;

    Date ts = Date::negativeInfinity();
    // copied from the LAL repo

    auto onSubexpression = [&] (const Coord & columnName,
                                const Coords & prefix,
                                const ExpressionValue & val)
    {
        ts.setMax(val.getEffectiveTimestamp());
        
        uint64_t hash = columnName.hash();
        if (!doneHashes.insert(hash).second)
            return true;

        // cerr << "got " << hash << endl;

        int bit = 0;
        for (int i = 0;  bit <= 63;  ++i, bit += functionConfig.numBits) {
            int bucket = (hash >> bit) & ((1ULL << functionConfig.numBits) - 1);
            ExcAssert(bucket >= 0 && bucket <= numBuckets());
            int val = (i % 2 ? -1 : 1);
            // cerr << "bit = " << bit << " bucket = " << bucket
            //     << " val = " << val << endl;
            result[bucket] += val;
        }

        return true;
    };

    input.columns.forEachSubexpression(onSubexpression);

    FunctionOutput foResult;
    RowValue rowVal;
    for(int i=0; i<result.size(); i++) {
        rowVal.push_back(make_tuple(ColumnName(ML::format("hashColumn%d", i)),
                                    CellValue(result[i]), ts));
    }
    
    return {ExpressionValue(rowVal)};
}

namespace {

static RegisterFunctionType<HashedColumnFeatureGenerator,
                            HashedColumnFeatureGeneratorConfig>
regHashedColFeatGenFunction(builtinPackage(),
                            "experimental.feature_generator.hashed_column",
                            "Generate hashed feature vector from the columns",
                            "functions/FeatureGeneratorHashedColumn.md.html",
                            nullptr /* static route */,
                            { MldbEntity::INTERNAL_ENTITY });

} // file scope

} // namespace MLDB
} // namespace Datacratic
