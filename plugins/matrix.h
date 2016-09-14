/** matrix.h                                                       -*- C++ -*-
    Jeremy Barnes, 5 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Dataset view as a dense+sparse matrix of values.  Rows are numbered by
    integers; columns numbered by integers and sparse values.
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/sql/cell_value.h"
#include "mldb/server/dataset_context.h"
#include "mldb/jml/stats/distribution.h"
#include "mldb/ml/svd_utils.h"
#include <boost/multi_array.hpp>


namespace Datacratic {
namespace MLDB {

struct SqlExpression;

enum ColumnOperator {
    COL_PRESENT,       ///< Boolean: row has the given value
    COL_EQUAL,         ///< Boolean: row is equal to the given value
    COL_VALUE          ///< Continuous: extract the value from the row, or 0 if not
};

DECLARE_ENUM_DESCRIPTION(ColumnOperator);

struct ColumnSpec {
    ColumnSpec(ColumnName columnName = ColumnName(),
               CellValue cellValue = CellValue(),
               ColumnOperator op = COL_VALUE,
               float offset = 0.0,
               float scale = 1.0)
        : columnName(std::move(columnName)),
          cellValue(std::move(cellValue)),
          op(op), offset(offset), scale(scale)
    {
    }

    ColumnName columnName;
    CellValue cellValue;
    ColumnOperator op;
    float offset;
    float scale;

    Utf8String getName() const
    {
        Utf8String result = columnName.toUtf8String();
        if (!cellValue.empty())
            result += " " + cellValue.toUtf8String();
        return result;
    }
};

DECLARE_STRUCTURE_DESCRIPTION(ColumnSpec);

struct ContinuousColumnInfo: public ColumnSpec {
    ContinuousColumnInfo(ColumnName columnName, int rowCount)
        : ColumnSpec(columnName, CellValue(), COL_VALUE),
          rowCount(rowCount)
    {
    }

    int rowCount;
};

struct SparseColumnInfo: public ColumnSpec {
    SparseColumnInfo(ColumnName columnName, CellValue value, int rowCount,
                     bool isContinuous)
        : ColumnSpec(columnName, value, isContinuous ? COL_VALUE: COL_EQUAL),
          rowCount(rowCount)
    {
    }

    int rowCount;
};

struct ExtractedRow {
    ExtractedRow()
    {
    }

    ExtractedRow(ML::distribution<float> continuous,
                 std::vector<std::pair<ColumnHash, CellValue> > sparse)
        : continuous(std::move(continuous)),
          sparse(std::move(sparse))
    {
    }

    RowHash rowHash;  /// To sort on
    ML::distribution<float> continuous;
    std::vector<std::pair<ColumnHash, CellValue> > sparse;
};

struct ClassifiedColumns {
    std::vector<ContinuousColumnInfo> continuousColumns;
    std::vector<SparseColumnInfo> sparseColumns;

    /** Contains the information on how to map the value of a column into a
        given sparse column.
    */
    struct ColumnIndexEntry {
        ColumnIndexEntry()
            : continuousIndex(-1)
        {
        }

        /// Column number for each discrete value
        std::map<CellValue, int> discreteIndex;

        /// Column number for continuous value.  -1 if it's discrete.
        int continuousIndex;

        bool isContinuous() const
        {
            return continuousIndex != -1;
        }
    };

    /** Look up the sparse column number for the given column with the
        given value.

        Returns the column index and a boolean indicating continuous (true)
        or discrete (false).
    */
    std::pair<int, bool>
    getSparseIndexForColumnValue(ColumnHash column, const CellValue & val) const
    {
        auto it = sparseIndex.find(column);
        if (it == sparseIndex.end())
            return { -1, false };

        if (it->second.isContinuous()) {
            if (!val.isNumber())
                return { -1, false };
            return { it->second.continuousIndex, true };
        }

        // Otherwise, look up our value
        auto jt = it->second.discreteIndex.find(val);
        if (jt == it->second.discreteIndex.end())
            return { -1, false };
        return { jt->second, false };
    }

    std::map<ColumnHash, ColumnIndexEntry> sparseIndex;

    Date modelTs;
};

struct FeatureBucket: public std::vector<ExtractedRow> {
    FeatureBucket()
        : startIndex(-1), endIndex(-1)
    {
    }

    ssize_t startIndex, endIndex;
};

struct FeatureBuckets: public std::vector<FeatureBucket> {
    FeatureBuckets(int numBuckets)
        : std::vector<FeatureBucket>(numBuckets),
          numExamples(0)
    {
    }

    size_t numExamples;
    Date modelTs;
};

enum ColumnType {
    CONTINUOUS_DENSE,  ///< Continuous values, stored in a dense array
    CONTINUOUS_SPARSE, ///< Continuous values, stored in a sparse array
    DISCRETE_SPARSE    ///< Discrete values, stored in a sparse array
};

struct ColumnIndexEntry: public ColumnSpec {

    ColumnIndexEntry()
        : columnType(CONTINUOUS_DENSE), mean(0.0), stddev(0.0), numExamples(0),
          numExamplesWithColumn(0)
    {
    }

    void initContinuousDense(int numExamplesWithColumn, int numExamples, ColumnName columnName)
    {
        this->columnType = CONTINUOUS_DENSE;
        this->numExamples = numExamples;
        this->numExamplesWithColumn = numExamplesWithColumn;
        this->columnName = std::move(columnName);
        this->cellValue = std::move(cellValue);
        this->op = COL_VALUE;
        continuousValues.clear();
        continuousValues.resize(numExamples);
    }

    void initSparse(int numExamplesWithColumn, int numExamples, ColumnName columnName, CellValue cellValue, ColumnOperator op)
    {
        this->op = op;
        this->columnType = op == COL_VALUE ? CONTINUOUS_SPARSE : DISCRETE_SPARSE;
        this->numExamples = numExamples;
        this->numExamplesWithColumn = numExamplesWithColumn;
        this->columnName = std::move(columnName);
        this->cellValue = std::move(cellValue);
        // nothing to do
    }

    void normalize()
    {
        using namespace std;

        switch (columnType) {
        case CONTINUOUS_DENSE:
            mean = continuousValues.total() / numExamplesWithColumn;
            stddev = continuousValues.std();

            continuousValues -= mean;
            offset = -mean;

            if (stddev != 0.0) {
                scale = 1.0 / stddev;
                continuousValues *= 1.0 / stddev;
            }

            //cerr << "continuous variable has mean " << mean << " stddev " << stddev << endl;
            break;

        case CONTINUOUS_SPARSE: {
            std::sort(sparseValues.begin(), sparseValues.end());

            double total = 0.0;

            for (auto & v: sparseValues) {
                total += v.second;
            }

            mean = total / sparseValues.size();
            offset = -mean;

            double devSqrTotal = 0.0;

            for (auto & v: sparseValues) {
                double dist = v.second - mean;
                devSqrTotal += dist * dist;
            }

            stddev = sqrt(devSqrTotal / sparseValues.size());

            double scale = 1.0;
            if (stddev != 0.0) {
                scale = 1.0 / stddev;
            }

            for (auto & v: sparseValues) {
                v.second -= mean;
                v.second *= scale;
            }
            
            //cerr << "sparse variable has mean " << mean << " stddev " << stddev << endl;
            break;
        }

        case DISCRETE_SPARSE:
            discreteValues.sort();
            discreteValues.compress();
            break;
        }
    }

    double correlation(const ColumnIndexEntry & other) const
    {
        //using namespace std;
        //cerr << "correlation between " << columnType << " and " << other.columnType
        //     << endl;

        switch (columnType) {
        case CONTINUOUS_DENSE:
            switch (other.columnType) {
            case CONTINUOUS_DENSE:
                return correlationContinuousContinuous(other);
            case CONTINUOUS_SPARSE:
                return correlationContinuousSparse(other);
            case DISCRETE_SPARSE:
                return correlationContinuousDiscrete(other);
            }
            break;
        case CONTINUOUS_SPARSE:
            switch (other.columnType) {
            case CONTINUOUS_DENSE:
                return other.correlationContinuousSparse(*this);
            case CONTINUOUS_SPARSE:
                return correlationSparseSparse(other);
            case DISCRETE_SPARSE:
                return correlationSparseDiscrete(other);
            }
            break;
        case DISCRETE_SPARSE:
            switch (other.columnType) {
            case CONTINUOUS_DENSE:
                return other.correlationContinuousDiscrete(*this);
            case CONTINUOUS_SPARSE:
                return other.correlationSparseDiscrete(*this);
            case DISCRETE_SPARSE:
                return correlationDiscreteDiscrete(other);
            }
        };

        throw ML::Exception("Unknown ColumnType");
    }

    double correlationContinuousContinuous(const ColumnIndexEntry & other) const
    {
        double result = continuousValues.dotprod(other.continuousValues)
            / numExamples;
        if (!std::isfinite(result))
            throw ML::Exception("non-finite correlation");
        //cerr << "column " << columnName << " and " << other.columnName
        //     << " have correlation " << result << endl;
        return result;
    }

    double correlationContinuousSparse(const ColumnIndexEntry & other) const
    {
        double total = 0.0;

        for (auto & v: other.sparseValues) {
            total += v.second * continuousValues[v.first];
        }

        return total / numExamples;
    }

    double correlationContinuousDiscrete(const ColumnIndexEntry & other) const
    {
        double total = 0.0;

        auto onIndex = [&] (int index)
            {
                total += continuousValues[index];
            };
        
        other.discreteValues.forEach(onIndex);

        double result = total / numExamples;

        //cerr << "seme-sparse column " << columnName << " and " << other.columnName
        //     << " " << other.cellValue << " have correlation " << result << endl;

        return result;
    }

    double correlationSparseSparse(const ColumnIndexEntry & other) const
    {
        double total = 0.0;

        using namespace std;
        //cerr << "sparse to sparse" << endl;
        //cerr << "sparseValus.size() = " << sparseValues.size() << endl;
        //cerr << "other.sparseValues.size() = " << other.sparseValues.size() << endl;

        auto it1 = sparseValues.begin(), end1 = sparseValues.end();
        auto it2 = other.sparseValues.begin(), end2 = other.sparseValues.end();

        while (it1 != end1 && it2 != end2) {
            int index1 = it1->first, index2 = it2->first;
            if (index1 == index2) {
                total += it1->second * it2->second;
                ++it1;
                ++it2;
            }
            else if (index1 < index2) {
                ++it1;
            }
            else ++it2;
        }

        double result = total / numExamples;

        return result;
    }

    double correlationSparseDiscrete(const ColumnIndexEntry & other) const
    {
        double total = 0.0;

        auto it = sparseValues.begin(), end = sparseValues.end();

        auto onIndex = [&] (int index)
            {
                while (it != end && it->first < index) {
                    ++it;
                }

                if (it == end)
                    return;

                if (it ->first == index)
                    total += it->second;
            };
        
        other.discreteValues.forEach(onIndex);

        double result = total / numExamples;

        return result;
    }

    double correlationDiscreteDiscrete(const ColumnIndexEntry & other) const
    {
        return 1.0 * discreteValues.calcOverlap(other.discreteValues, HAMMING,
                                                true /* short cct */)
            / numExamples;
    }

    ColumnType columnType;
    double mean;
    double stddev;
    int numExamples;
    int numExamplesWithColumn;

    ML::distribution<float> continuousValues;
    std::vector<std::pair<int, float> > sparseValues;
    SvdColumnEntry discreteValues;
};

struct ColumnIndexEntries: public std::vector<ColumnIndexEntry> {
    ColumnIndexEntries(const ClassifiedColumns & columns,
                       size_t numExamples)
        : std::vector<ColumnIndexEntry>(columns.continuousColumns.size()
                                        + columns.sparseColumns.size())
    {
        for (unsigned i = 0;  i < columns.continuousColumns.size();  ++i) {
            operator [] (i).initContinuousDense(columns.continuousColumns[i].rowCount,
                                                numExamples,
                                                columns.continuousColumns[i].columnName);
        }
        
        for (unsigned index = 0;  index < columns.sparseColumns.size();  ++index) {
            at(columns.continuousColumns.size() + index)
                .initSparse(columns.sparseColumns[index].rowCount,
                            numExamples,
                            columns.sparseColumns[index].columnName,
                            columns.sparseColumns[index].cellValue,
                            columns.sparseColumns[index].op);
        }
    }

    Date modelTs;
};

struct ColumnCorrelations {
    ColumnCorrelations()
    {
    }

    template<typename It>
    ColumnCorrelations(It begin, It end)
        : columns(begin, end),
          correlations(boost::extents[columns.size()][columns.size()])
    {
    }

    std::vector<ColumnSpec> columns;
    boost::multi_array<float, 2> correlations;

    size_t columnCount() const
    {
        return columns.size();
    }

    Date modelTs;
};


ClassifiedColumns classifyColumns(const SelectExpression & select,
                                  const Dataset & from,
                                  const WhenExpression & when,
                                  const SqlExpression & where,
                                  const OrderByExpression & orderBy,
                                  ssize_t offset,
                                  ssize_t limit);

FeatureBuckets extractFeaturesFromEvents(const Dataset & dataset,
                                         const ClassifiedColumns & columns);

FeatureBuckets extractFeaturesFromRows(const SelectExpression & select,
                                       const Dataset & dataset,
                                       const WhenExpression & whenClause,
                                       std::shared_ptr<SqlExpression> whereClause,
                                       const OrderByExpression & orderBy, 
                                       ssize_t offset,
                                       ssize_t limit,
                                       const ClassifiedColumns & columns);

ColumnIndexEntries
invertFeatures(const ClassifiedColumns & columns,
               const FeatureBuckets & featureBuckets);
    
ColumnCorrelations
calculateCorrelations(const ColumnIndexEntries & columnIndex,
                      int numBasisVectors);


} // namespace MLDB
} // namespace Datacratic
