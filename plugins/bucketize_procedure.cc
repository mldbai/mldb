// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
/**
 * bucketize_procedure.cc
 * Mich, 2015-10-27
 * Copyright (c) 2015 Datacratic Inc. All rights reserved.
 **/

#include "bucketize_procedure.h"
#include "mldb/server/mldb_server.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/server/dataset_context.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/jml/utils/worker_task.h"
#include "mldb/server/function_contexts.h"
#include "mldb/server/bound_queries.h"
#include "mldb/sql/table_expression_operations.h"
#include "mldb/sql/join_utils.h"
#include "mldb/sql/execution_pipeline.h"
#include "mldb/arch/backtrace.h"
#include "mldb/types/any_impl.h"
#include "mldb/server/per_thread_accumulator.h"
#include "mldb/types/date.h"
#include "mldb/sql/sql_expression.h"
#include <memory>

using namespace std;


namespace Datacratic {
namespace MLDB {

BucketizeProcedureConfig::
BucketizeProcedureConfig()
    : when(WhenExpression::TRUE), where(SqlExpression::TRUE)
{
    outputDataset.withType("sparse.mutable");
}

DEFINE_STRUCTURE_DESCRIPTION(BucketizeProcedureConfig);

BucketizeProcedureConfigDescription::
BucketizeProcedureConfigDescription()
{
    addFieldDesc("inputDataset", &BucketizeProcedureConfig::inputDataset,
                 "Input dataset. This must be an existing dataset.",
                 makeInputDatasetDescription());
    addField("outputDataset", &BucketizeProcedureConfig::outputDataset,
             "Output dataset configuration. This may refer either to an "
             "existing dataset, or a fully specified but non-existing dataset "
             "which will be created by the procedure.",
             PolyConfigT<Dataset>().withType("sparse.mutable"));
    addField("orderBy", &BucketizeProcedureConfig::orderBy,
             "The order to use to rank the rows prior to bucketization.");
    addField("when", &BucketizeProcedureConfig::when,
             "Boolean expression determining which tuples from the dataset "
             "to keep based on their timestamps",
             WhenExpression::TRUE);
    addField("where", &BucketizeProcedureConfig::where,
             "Boolean expression to choose which row to select. In almost all "
             "cases this should be set to restrict the query to the part of "
             "the dataset that is interesting in the context of the query.",
             SqlExpression::TRUE);
    addField("percentileBuckets", &BucketizeProcedureConfig::percentileBuckets,
             "Key/ranges of the buckets to create. Buckets ranges can share "
             "start and end values but cannot overlap such that a row can "
             "belong to multiple buckets. "
             "E.g. {\"a\":[0, 50], \"b\": [50, 100]} will give two buckets: "
             "\"a\" has rows where 0% < rank/count <= 50% and "
             "\"b\" has rows where 50% < rank/count <= 100%, where the 'rank' "
             "is based on the orderBy parameter.");
    addParent<ProcedureConfig>();

    onPostValidate = [&] (BucketizeProcedureConfig * cfg,
                          JsonParsingContext & context)
    {
        vector<pair<float, float>> ranges;
        for (const auto & range: cfg->percentileBuckets) {
            ranges.push_back(range.second);
        }
        auto sorter = [](pair<float, float> a, pair<float, float> b)
        {
            return a.first < b.first;
        };
        sort(ranges.begin(), ranges.end(), sorter);

        auto last = make_pair(-1.0, -1.0);
        for (const auto & range: ranges) {
            if (range.first < 0) {
                throw ML::Exception(
                    "Invalid percentileBucket [%f, %f]: lower bound must be "
                    "greater or equal to 0", range.first, range.second);
            }
            if (range.second > 100) {
                throw ML::Exception(
                    "Invalid percentileBucket [%f, %f]: higher bound must be "
                    "lower or equal to 1", range.first, range.second);
            }
            if (range.first >= range.second) {
                throw ML::Exception(
                    "Invalid percentileBucket [%f, %f]: higher bound must  "
                    "be greater than lower bound", range.first, range.second);
            }
            if (range.first < last.second) {
                throw ML::Exception(
                    "Invalid percentileBucket: [%f, %f] is overlapping with "
                    "[%f, %f]", last.first, last.second, range.first,
                    range.second);
            }
            last = range;
        }
    };
}

BucketizeProcedure::
BucketizeProcedure(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    procedureConfig = config.params.convert<BucketizeProcedureConfig>();
}

RunOutput
BucketizeProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    SqlExpressionMldbContext context(server);

    auto boundDataset = procedureConfig.inputDataset->bind(context);

    SelectExpression select(SelectExpression::parse("1"));
    vector<shared_ptr<SqlExpression> > calc;
    ssize_t offset = 0;
    ssize_t limit = -1;

    vector<Id> orderedRowNames;
    auto getSize = [&] (const MatrixNamedRow & row,
                        const vector<ExpressionValue> & calc)
    {
        orderedRowNames.emplace_back(row.rowName);
        return true;
    };

    BoundSelectQuery(select,
                     *boundDataset.dataset,
                     boundDataset.asName,
                     procedureConfig.when,
                     procedureConfig.where,
                     procedureConfig.orderBy,
                     calc)
        .execute(getSize, offset, limit, onProgress);
    int64_t rowCount = orderedRowNames.size();
    cerr << "Row count: " << rowCount  << endl;

    std::shared_ptr<Dataset> output;
    if (!procedureConfig.outputDataset.type.empty() || !procedureConfig.outputDataset.id.empty()) {
        output = createDataset(server, procedureConfig.outputDataset, nullptr, true /*overwrite*/);
    }

    typedef tuple<ColumnName, CellValue, Date> cell;
    PerThreadAccumulator<vector<pair<RowName, vector<cell>>>> accum;

    for (const auto & mappedRange: procedureConfig.percentileBuckets) {
        string bucketName(mappedRange.first);
        auto applyFct = [&] (int64_t index)
        {
            std::vector<cell> cols;
            cols.emplace_back(ColumnName("bucket"),
                              bucketName,
                              Date::negativeInfinity());

            auto & rows = accum.get();
            rows.reserve(1024);
            rows.emplace_back(orderedRowNames[index], cols);

            if (rows.size() >= 1024) {
                output->recordRows(rows);
                rows.clear();
            }
        };
        auto range = mappedRange.second;

        //Make sure that numerical issues dont let 100 percentile go out of bound
        int64_t lowerBound = range.second == 0 ? 0 : int64_t(range.first / 100 * rowCount);
        int64_t higherBound = range.second == 100 ? rowCount : int64_t(range.second / 100 * rowCount);
        
        ExcAssert(higherBound <= rowCount);

        cerr << "Bucket " << bucketName << " from " << lowerBound
             << " to " << higherBound << endl;

        ML::run_in_parallel_blocked(lowerBound, higherBound, applyFct);
    }

    // record remainder
    accum.forEach([&] (vector<pair<RowName, vector<cell>>> * rows)
    {
        output->recordRows(*rows);
    });

    output->commit();
    return output->getStatus();
}

Any
BucketizeProcedure::
getStatus() const
{
    return Any();
}

static RegisterProcedureType<BucketizeProcedure, BucketizeProcedureConfig>
regBucketizeProcedure(
    builtinPackage(),
    "bucketize",
    "Assign buckets based on percentile ranges over a sorted dataset",
    "procedures/BucketizeProcedure.md.html");
 

} // namespace MLDB
} // namespace Datacratic
