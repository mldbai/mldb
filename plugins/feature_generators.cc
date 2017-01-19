/** feature_generators.cc
    Francois Maillet, 27 juillet 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
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
#include "utils/json_utils.h"
#include "mldb/ext/highwayhash.h"
#include "mldb/utils/log.h"

using namespace std;


namespace MLDB {

constexpr HashSeed defaultSeedStable { .u64 = { 0x1958DF94340e7cbaULL, 0x8928Fc8B84a0ULL } };


/*****************************************************************************/
/* HASHED COLUMN FEAT GEN CONFIG                                             */
/*****************************************************************************/

DEFINE_ENUM_DESCRIPTION(HashingMode);

HashingModeDescription::
HashingModeDescription()
{
    addValue("columns",          COLUMNS, "Hash only the column names");
    addValue("columnsAndValues", COLUMNS_AND_VALUES, "Hash the concatenation "
                                                     "of the column names and "
                                                     "the cell values");
}

DEFINE_STRUCTURE_DESCRIPTION(HashedColumnFeatureGeneratorConfig);

HashedColumnFeatureGeneratorConfigDescription::
HashedColumnFeatureGeneratorConfigDescription()
{
    addField("numBits", &HashedColumnFeatureGeneratorConfig::numBits,
             "Number of bits to use for the hash. The number of resulting "
                "buckets will be $$2^{\\text{numBits}}$$.", 8);
    addField("mode", &HashedColumnFeatureGeneratorConfig::mode,
            "Hashing mode to use. Controls what gets hashed.",
            COLUMNS);
}

/*****************************************************************************/
/* HASHED COLUMN FEAT GEN                                                    */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(FeatureGeneratorInput);

FeatureGeneratorInputDescription::FeatureGeneratorInputDescription()
{
    addField("columns", &FeatureGeneratorInput::columns,
             "Undocumented");
}

DEFINE_STRUCTURE_DESCRIPTION(FeatureGeneratorOutput);

FeatureGeneratorOutputDescription::FeatureGeneratorOutputDescription()
{
    addField("hash", &FeatureGeneratorOutput::hash,
             "Undocumented");
}

HashedColumnFeatureGenerator::
HashedColumnFeatureGenerator(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
    : BaseT(owner, config)
{
    functionConfig = config.params.convert<HashedColumnFeatureGeneratorConfig>();

    for(int i=0; i<numBuckets(); i++) {
        outputColumns.emplace_back(ColumnPath(MLDB::format("hashColumn%d", i)),
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
call(FeatureGeneratorInput input) const
{
    distribution<float> result(numBuckets());

    Lightweight_Hash_Set<uint64_t> doneHashes;

    Date ts = Date::negativeInfinity();

    auto onColumn = [&] (const PathElement & columnName,
                         const ExpressionValue & val)
    {
        ts.setMax(val.getEffectiveTimestamp());

        uint64_t hash;
        if(functionConfig.mode == COLUMNS) {
            hash = columnName.hash();
        }
        else if(functionConfig.mode == COLUMNS_AND_VALUES) {
            Utf8String str(columnName.toUtf8String() + "::" + val.toUtf8String());
            // Keep sip hash so that old classifiers still work
            hash = sipHash(defaultSeedStable.u64, str.rawData(), str.rawLength());
        }
        else {
            throw MLDB::Exception("Unsupported hashing mode");
        }

        if (!doneHashes.insert(hash).second)
            return true;

        TRACE_MSG(logger) << "got " << hash;

        int bit = 0;
        for (int i = 0;  bit <= 63;  ++i, bit += functionConfig.numBits) {
            int bucket = (hash >> bit) & ((1ULL << functionConfig.numBits) - 1);
            ExcAssert(bucket >= 0 && bucket <= numBuckets());
            int val = (i % 2 ? -1 : 1);
            TRACE_MSG(logger) << "bit = " << bit << " bucket = " << bucket
                              << " val = " << val;
            result[bucket] += val;
        }

        return true;
    };

    input.columns.forEachColumn(onColumn);

    ExpressionValue foResult;
    RowValue rowVal;
    for(int i=0; i<result.size(); i++) {
        rowVal.push_back(make_tuple(ColumnPath(MLDB::format("hashColumn%d", i)),
                                    CellValue(result[i]), ts));
    }

    return {ExpressionValue(rowVal)};
}

namespace {

static RegisterFunctionType<HashedColumnFeatureGenerator,
                            HashedColumnFeatureGeneratorConfig>
regHashedColFeatGenFunction(builtinPackage(),
                            "feature_hasher",
                            "Feature hashing feature generator",
                            "functions/FeatureHashingFunction.md.html",
                            nullptr /* static route */);

} // file scope

} // namespace MLDB


