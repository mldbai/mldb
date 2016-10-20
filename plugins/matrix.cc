// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** matrix.cc
    Jeremy Barnes, 5 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "matrix.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/base/parallel.h"
#include "mldb/arch/timers.h"
#include "mldb/server/analytics.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/server/dataset_context.h"
#include "mldb/http/http_exception.h"
#include "mldb/jml/utils/less.h"
#include "mldb/utils/log.h"
#include "mldb/rest/cancellation_exception.h"
#include <mutex>

using namespace std;

namespace MLDB {

namespace {
   std::unordered_map<ColumnPath, ColumnStats>
   getColumnStats(const SelectExpression & select,
                  const Dataset & from,
                  const WhenExpression & when,
                  const SqlExpression & where,
                  const OrderByExpression & orderBy,
                  ssize_t offset,
                  ssize_t limit,
                  const std::function<bool (const Json::Value &)> & onProgress) {

       std::unordered_map<ColumnPath, ColumnStats> stats;
       
       auto onRow = [&stats] (NamedRowValue & output_) {
           MatrixNamedRow output = output_.flattenDestructive();
           for (auto & col : output.columns) {
               auto columnPath = get<0>(col);
               auto cellValue = get<1>(col);
               auto it = stats.find(columnPath);
               if (it == stats.end()) {
                   auto & colStats = stats[columnPath];
                   colStats.isNumeric_ = cellValue.isNumber();
               }
               auto & colStats = stats[columnPath];
               colStats.isNumeric_ = colStats.isNumeric_ && cellValue.isNumber();
               colStats.rowCount_ +=1;
               // since there could be several values for a column in the same row 
               // this is more a value count than a row count!
               colStats.values[cellValue].rowCount_ += 1;
           }

           return true;
       };

       if (!iterateDataset(select, from, "", when, where, 
                          {onRow, false /*processInParallel*/}, 
                          orderBy, offset, limit, onProgress).first) {
           throw CancellationException("getColumnStats was cancelled");
       }
       return stats;
   }
}

DEFINE_ENUM_DESCRIPTION(ColumnOperator);

ColumnOperatorDescription::
ColumnOperatorDescription()
{
    addValue("PRESENT", COL_PRESENT, "Boolean: Column is present");
    addValue("EQUAL", COL_EQUAL, "Boolean: Column is equal to value");
    addValue("VALUE", COL_VALUE, "Continuous: Extract value from column");
}

DEFINE_STRUCTURE_DESCRIPTION(ColumnSpec);

ColumnSpecDescription::
ColumnSpecDescription()
{
    addField("columnName", &ColumnSpec::columnName,
             "Name of column");
    addField("cellValue", &ColumnSpec::cellValue,
             "Value of cell");
    addField("op", &ColumnSpec::op,
             "Operator");
    addField("offset", &ColumnSpec::offset, "Offset to add to value");
    addField("scale", &ColumnSpec::scale, "Amount to scale value by");
}

ClassifiedColumns
classifyColumns(const SelectExpression & select_,
                const Dataset & dataset,
                const WhenExpression & when,
                const SqlExpression & where,
                const OrderByExpression & orderBy,
                ssize_t offset,
                ssize_t limit,
                std::shared_ptr<spdlog::logger> logger,
                std::function<bool (const Json::Value &)> & onProgress)
{
    // Get a list of the columns we want to use, by parsing the select
    // expression.  Note that only direct expression values will work.

    // Default is all columns
    SelectExpression select = select_;
    if (select.clauses.empty())
        select = SelectExpression::parse("*");

    SqlExpressionDatasetScope context(dataset, "");

    auto boundSelect = select.bind(context);

    std::vector<ColumnPath> selectedColumnsVec = boundSelect.info->allColumnNames();
    std::set<ColumnPath> selectedColumns(selectedColumnsVec.begin(),
                                         selectedColumnsVec.end());
    if (boundSelect.info->getSchemaCompletenessRecursive() == SCHEMA_OPEN)
        WARNING_MSG(logger) << "non-enumerated columns will not be used "
            "in correlation training";
    
    DEBUG_MSG(logger) << "selected " << selectedColumns.size() << " columns";
    
    // Classify the columns into two different types:

    std::vector<ContinuousColumnInfo> continuousColumns;
    std::vector<SparseColumnInfo> sparseColumns;

    std::unordered_map<ColumnPath, ColumnStats> stats = 
        getColumnStats(select, dataset, when, where, orderBy, offset, limit, onProgress);
 
    DEBUG_MSG(logger) << "stats size " << stats.size();

    size_t rowCount = dataset.getMatrixView()->getRowCount();


    for (auto & colStats : stats) {

        // If it wasn't in our selected columns, we don't keep it       
        if (!selectedColumns.count(colStats.first))  
            continue;

        DEBUG_MSG(logger) << "column " << colStats.first << " has " << colStats.second.rowCount()
                          << " values set" << " numeric = " << colStats.second.isNumeric()
                          << " vals "
                          << colStats.second.values.size();
            
        bool isDense = colStats.second.rowCount() == rowCount;

        //TODO - what to do with the case where a row contains many values
        
        if (colStats.second.isNumeric() && isDense && colStats.second.values.size() > 1) {
            // Candidate for a dense continuous column
            continuousColumns.emplace_back(colStats.first, colStats.second.rowCount());
        }
        else if (colStats.second.isNumeric() && colStats.second.values.size() > 1) {
            // Candidate for a discrete, real valued column
            sparseColumns.emplace_back(colStats.first, CellValue(),
                                       colStats.second.rowCount(),
                                       true /* isContinuous */);
        }
        else {
            //cerr << "sparse column" << endl;
            // Either single or dual-valued, or always empty
            // Look through the values of this feature
            for (auto & v: colStats.second.values) {
                sparseColumns.emplace_back(colStats.first, v.first,
                                           v.second.rowCount(),
                                           false /* isContinuous */);
            }
        }
    }

    // Step 3: Select all of our basis columns for our SVD
    std::map<ColumnHash, ClassifiedColumns::ColumnIndexEntry> sparseIndex;

    std::sort(sparseColumns.begin(), sparseColumns.end(),
              [&] (const SparseColumnInfo & i1,
                   const SparseColumnInfo & i2)
              {
                  return i1.rowCount > i2.rowCount;
              });
    
    //size_t nColumns = std::min<size_t>(1000, sparseColumns.size());

    //cerr << "choosing the " << nColumns << " of " << sparseColumns.size()
    //     << " columns with at least " << sparseColumns[nColumns - 1].rowCount
    //     << " rows" << endl;

    for (unsigned i = 0;  i < sparseColumns.size();  ++i) {
        auto & entry = sparseIndex[sparseColumns[i].columnName];

        if (sparseColumns[i].op == COL_VALUE) {
            entry.continuousIndex = i;
        }
        else {
            entry.discreteIndex[sparseColumns[i].cellValue] = i;
        }
    }

    //cerr << "sparseIndex.size() = " << sparseIndex.size() << endl;

    ClassifiedColumns result;
    result.continuousColumns = std::move(continuousColumns);
    result.sparseColumns = std::move(sparseColumns);
    result.sparseIndex = std::move(sparseIndex);

    return result;
}

FeatureBuckets 
extractFeaturesFromRows(const SelectExpression & select,
                        const Dataset & dataset,
                        const WhenExpression & when,
                        std::shared_ptr<SqlExpression> where,
                        const OrderByExpression & orderBy, 
                        ssize_t offset,
                        ssize_t limit,
                        const ClassifiedColumns & columns,
                        std::shared_ptr<spdlog::logger> logger,
                        std::function<bool (const Json::Value &)> & onProgress)
{
    static const int numBuckets = 32;

    FeatureBuckets featureBuckets(numBuckets);
    std::mutex bucketMutexes[numBuckets];
    std::atomic<size_t> numExamples(0);

    Timer timer;

    DEBUG_MSG(logger) << "extracting values";

    // Get an index of ColumnHash to dense value
    std::unordered_map<ColumnHash, int> continuousIndex;
    for (unsigned i = 0;  i < columns.continuousColumns.size();  ++i) {
        continuousIndex[columns.continuousColumns[i].columnName] = i;
    }

    Date modelTs[numBuckets];
    std::fill(modelTs, modelTs + numBuckets, Date::negativeInfinity());

    // Extract entire rows
    auto onRow = [&] (NamedRowValue & output_)
        {
            MatrixNamedRow output = output_.flattenDestructive();

            ExtractedRow row;
            row.continuous.resize(columns.continuousColumns.size(), 0.0);
            
            Date rowTs = Date::negativeInfinity();
            for (auto & c: output.columns) {
                ColumnHash columnHash = std::get<0>(c);
                const CellValue & cellValue = std::get<1>(c);
                Date ts = std::get<2>(c);
                rowTs.setMax(ts);
                auto it = continuousIndex.find(columnHash);
                if (it == continuousIndex.end())
                    row.sparse.emplace_back(columnHash, cellValue);
                else row.continuous[it->second] = cellValue.toDouble();
            }

            int bucket = output.rowHash.hash() % numBuckets;
            row.rowHash = output.rowHash;

            std::unique_lock<std::mutex> guard(bucketMutexes[bucket]);
            featureBuckets[bucket].emplace_back(std::move(row));

            ++numExamples;

            modelTs[bucket].setMax(rowTs);

            return true;
        };
    iterateDataset(select, dataset, "", when, *where, 
                   {onRow, true /*processInParallel*/}, orderBy, offset, limit, onProgress);

    DEBUG_MSG(logger) << "done extracting values in " << timer.elapsed();

    timer.restart();

    // Sort the feature buckets so it's reproducible
    auto sortBucket = [&] (int i)
        {
            auto & bucket = featureBuckets[i];
            std::sort(bucket.begin(), bucket.end(), 
                      [] (const ExtractedRow & r1,
                          const ExtractedRow & r2) -> bool
                          {
                              return ML::less_all(r1.rowHash, r2.rowHash,
                                                  ((const std::vector<float> &)r1.continuous),
                                                  ((const std::vector<float> &)r2.continuous),
                                                  r1.sparse, r2.sparse);
                          });
        };
    parallelMap(0, featureBuckets.size(), sortBucket);

    DEBUG_MSG(logger) << "done sorting buckets in " << timer.elapsed();
    
    featureBuckets.numExamples = numExamples;

    Date tsOut = Date::negativeInfinity();
    for (auto & ts: modelTs)
        tsOut.setMax(ts);
    featureBuckets.modelTs = tsOut;
    
    int n = 0;
    for (auto & b: featureBuckets) {
        b.startIndex = n;
        n += b.size();
        b.endIndex = n;
    }

    ExcAssertEqual(n, numExamples);

#if 0
    {
        static int n = 0;
        cerr << "saving buckets " << n << endl;
        filter_ostream stream(MLDB::format("buckets-%d.json", n++));
        stream << "numExamples = " << featureBuckets.numExamples << endl;
        for (unsigned i = 0;  i < featureBuckets.size();  ++i) {
            stream << "bucket " << i << endl;
            auto & b = featureBuckets[i];
            stream << b.startIndex << " " << b.endIndex << endl;
            for (unsigned j = 0;  j < b.size();  ++j) {
                auto & r = b[j];
                stream << r.continuous << " " << jsonEncode(r.sparse) << endl;
            }
        }
        cerr << "done saving buckets " << endl;
    }
#endif

    return featureBuckets;
}

ColumnIndexEntries
invertFeatures(const ClassifiedColumns & columns,
               const FeatureBuckets & featureBuckets,
               std::shared_ptr<spdlog::logger> logger,
               std::function<bool (const Json::Value &)> & onProgress)
{
    Timer timer;

    int numContinuousColumns = columns.continuousColumns.size();
    int numSparseColumns = columns.sparseColumns.size();

    ColumnIndexEntries result(columns, featureBuckets.numExamples);
    result.modelTs = columns.modelTs;

    // Index of examples for each discrete behaviour
    std::mutex discreteValuesLock;

    std::atomic<uint64_t> bucketCount(0);
    size_t bucketNum = featureBuckets.size();

    auto doBucket = [&] (int n)
        {
            // For each discrete column, a list of the indexes of this bucket
            // that contain the feature.
            std::vector<std::vector<int> > bucketDiscreteIndexes(numSparseColumns);
            std::vector<std::vector<std::pair<int, float> > > bucketSparseIndexes(numSparseColumns);
            
            int index = featureBuckets[n].startIndex;
            ++bucketCount;
            if (onProgress) {
                Json::Value progress;
                progress["percent"] = (float) bucketCount / bucketNum;
                if (!onProgress(progress))
                    return false;
            }

            for (const ExtractedRow & entry: featureBuckets[n]) {
                //cerr << "continuous " << entry.continuous.size() << " sparse "
                //     << entry.sparse.size() << endl;
                    
                for (unsigned i = 0;  i < entry.continuous.size();  ++i) {
                    result[i].continuousValues[index] = entry.continuous[i];
                }
                for (auto col_val: entry.sparse) {

                    int sparseColumnNum;
                    bool isContinuous;
                    
                    std::tie(sparseColumnNum, isContinuous)
                        = columns.getSparseIndexForColumnValue(col_val.first,
                                                               col_val.second);
                    
                    if (sparseColumnNum == -1)
                        continue;

                    if (isContinuous)
                        bucketSparseIndexes[sparseColumnNum]
                            .emplace_back(index, col_val.second.toDouble());
                    else 
                        bucketDiscreteIndexes[sparseColumnNum].push_back(index);
                }
                
                ++index;
            }
            
            ExcAssertEqual(index, featureBuckets[n].endIndex);

            std::unique_lock<std::mutex> guard(discreteValuesLock);

            for (unsigned i = 0;  i < numSparseColumns;  ++i) {
                
                //cerr << "column " << i << " discrete " << bucketDiscreteIndexes[i].size()
                //<< " sparse " << bucketSparseIndexes[i].size() << endl;

                if (bucketDiscreteIndexes[i].size() > 0) {
                    result[i + numContinuousColumns].discreteValues
                        .insert(bucketDiscreteIndexes[i].begin(),
                                bucketDiscreteIndexes[i].end());
                }
                else {
                    result[i + numContinuousColumns].sparseValues
                        .insert(result[i + numContinuousColumns].sparseValues.end(),
                                bucketSparseIndexes[i].begin(),
                                bucketSparseIndexes[i].end());
                }
            }

            return true;
        };
    
    parallelMapHaltable(0, featureBuckets.size(), doBucket);

    DEBUG_MSG(logger) << "done indexes and correlations in " << timer.elapsed();

    auto sortIndex = [&] (int i)
        {
            result[i].normalize();
        };

    parallelMap(0, result.size(), sortIndex);
    
    DEBUG_MSG(logger) << "done feature matrix inversion" << timer.elapsed();

    return result;
}

ColumnCorrelations
calculateCorrelations(const ColumnIndexEntries & columnIndex,
                      int numBasisVectors)
{
    // Create a dense basis for a SVD
    // This is A^2, so has essentially cross-correlations between the most common features
    // This has entries as follows:
    // - for two dense features: dot product between unit variance, zero mean versions
    // - for one dense and one sparse: sum of unit variance, zero mean versions of the dense
    //   where the sparse is active
    // - for two sparse: hamming distance
    //

    Timer timer;

    int numColumns = std::min<int>(numBasisVectors, columnIndex.size());

    cerr << "calculating dense correlation matrix between " << numColumns
         << " basis vectors" << endl;
    
    ColumnCorrelations result(columnIndex.begin(), columnIndex.begin() + numColumns);
    result.modelTs = columnIndex.modelTs;

    vector<pair<int, int> > featurePairs;
    for (unsigned i = 0;  i < numColumns;  ++i) {
        result.correlations[i][i] = 1.0;
        for (unsigned j = 0;  j <= i;  ++j) {
            featurePairs.emplace_back(i, j);
        }
    }

    cerr << "processing " << featurePairs.size() << " correlations" << endl;

    auto doCorrelation = [&] (int n)
        {
            int i = featurePairs[n].first;
            int j = featurePairs[n].second;

            result.correlations[i][j]
                = result.correlations[j][i] 
                = columnIndex[i].correlation(columnIndex[j]);

#if 0
            static std::mutex mutex;
            std::unique_lock<std::mutex> guard(mutex);
            cerr << "i = " << i << " j = " << j << " n = " << n << endl;
            cerr << "correlation of " << columnIndex[i].getName()
            << " and " << columnIndex[j].getName()
            << " is " << result.correlations[i][j] << endl;
#endif
        };
    
    parallelMap(0, featurePairs.size(), doCorrelation);

    cerr << timer.elapsed() << endl;

#if 0
    for (unsigned i = 0;  i < numColumns;  ++i) {
        cerr << "correlation between " << columnIndex[0].getName() << " and "
             << columnIndex[i].getName() << " is " << result.correlations[0][i]
             << endl;
    }
#endif

    cerr << "done processing correlations" << endl;

    return result;
}

} // namespace MLDB

