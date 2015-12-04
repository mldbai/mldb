// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** transposed_dataset.cc                                              -*- C++ -*-
    Jeremy Barnes, 28 February 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "mldb/sql/sql_expression.h"
#include "transposed_dataset.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/structure_description.h"

using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* TRANSPOSED DATASET CONFIG                                                 */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(TransposedDatasetConfig);

TransposedDatasetConfigDescription::
TransposedDatasetConfigDescription()
{
    nullAccepted = true;

    addField("dataset", &TransposedDatasetConfig::dataset,
             "Dataset to transpose");
}


/*****************************************************************************/
/* TRANSPOSED INTERNAL REPRESENTATION                                        */
/*****************************************************************************/


struct TransposedDataset::Itl
    : public MatrixView, public ColumnIndex {
    
    /// Dataset that it was constructed with
    std::shared_ptr<Dataset> dataset;

    std::shared_ptr<MatrixView> matrix;
    std::shared_ptr<ColumnIndex> index;
    size_t columnCount;

    Itl(MldbServer * server, std::shared_ptr<Dataset> dataset)
        : dataset(dataset),
          matrix(dataset->getMatrixView()),
          index(dataset->getColumnIndex()),
          columnCount(matrix->getColumnNames().size())
    {
    }

    static RowHash colToRow(ColumnHash col)
    {
        return RowHash(col.hash());
    }

    static const RowName & colToRow(const ColumnName & col)
    {
        return col;
    }

    static std::vector<std::tuple<RowName, CellValue, Date> >
    colToRow(const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals)
    {
        std::vector<std::tuple<RowName, CellValue, Date> > result;
        result.reserve(vals.size());
        for (auto & v: vals)
            result.emplace_back(colToRow(std::get<0>(v)),
                                std::get<1>(v), std::get<2>(v));
        return result;
    }
    
    static std::vector<std::tuple<ColumnName, CellValue, Date> >
    rowToCol(const std::vector<std::tuple<RowName, CellValue, Date> > & vals)
    {
        std::vector<std::tuple<ColumnName, CellValue, Date> > result;
        result.reserve(vals.size());
        for (auto & v: vals)
            result.emplace_back(rowToCol(std::get<0>(v)),
                                std::get<1>(v), std::get<2>(v));
        return result;
    }
    
    static ColumnHash rowToCol(RowHash row)
    {
        return ColumnHash(row.hash());
    }

    static const ColumnName & rowToCol(const RowName & row)
    {
        return std::move(row);
    }

    virtual std::vector<RowName>
    getRowNames(ssize_t start = 0, ssize_t limit = -1) const
    {
        vector<ColumnName> cols = matrix->getColumnNames();

        vector<RowName> result;
        for (unsigned i = 0;  i < cols.size();  ++i) {
            if (i < start)
                continue;
            if (limit != -1 && result.size() >= limit)
                break;
            result.push_back(colToRow(cols[i]));
        }
        
        return cols;
    }

    virtual std::vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const
    {
        vector<RowHash> result;
        for (auto & n: getRowNames()) {
            result.emplace_back(n);
        }
        return result;
    }

    virtual bool knownRow(const RowName & rowName) const
    {
        return index->knownColumn(rowToCol(rowName));
    }

    virtual bool knownColumn(const ColumnName & columnName) const
    {
        return matrix->knownRow(colToRow(columnName));
    }

    virtual RowName getRowName(const RowHash & row) const
    {
        return matrix->getColumnName(rowToCol(row));
    }

    virtual ColumnName getColumnName(ColumnHash column) const
    {
        return matrix->getRowName(colToRow(column));
    }

    virtual MatrixNamedRow getRow(const RowName & rowName) const
    {
        MatrixColumn col = index->getColumn(rowToCol(rowName));
        MatrixNamedRow result;
        result.rowName = colToRow(col.columnName);
        result.rowHash = colToRow(col.columnHash);
        result.columns = rowToCol(col.rows);
        return result;
    }

    virtual std::vector<ColumnName> getColumnNames() const
    {
        std::vector<ColumnName> result;

        for (auto & c: matrix->getRowNames())
            result.emplace_back(rowToCol(c));
        
        return result;
    }

    virtual const ColumnStats &
    getColumnStats(const ColumnName & columnName, ColumnStats & stats) const
    {
        auto row = matrix->getRow(colToRow(columnName));

        stats = ColumnStats();

        ML::Lightweight_Hash_Set<ColumnHash> columns;
        bool oneOnly = true;
        bool isNumeric = true;

        for (auto & c: row.columns) {
            ColumnHash ch = std::get<0>(c);
            const CellValue & v = std::get<1>(c);

            if (!columns.insert(ch).second)
                oneOnly = false;
            
            if (!v.isNumber())
                isNumeric = false;
            
            // TODO: not really true...
            stats.values[v].rowCount_ += 1;
        }

        stats.isNumeric_ = isNumeric && !row.columns.empty();
        stats.rowCount_ = columns.size();
        stats.atMostOne_ = oneOnly;
        return stats;
    }

    virtual uint64_t getColumnRowCount(const ColumnName & column) const
    {
        return matrix->getRowColumnCount(colToRow(column));
    }

    virtual uint64_t getRowColumnCount(const RowName & row) const
    {
        return index->getColumnRowCount(rowToCol(row));
    }

    /** Return the value of the column for all rows and timestamps. */
    virtual MatrixColumn getColumn(const ColumnName & column) const
    {
        auto row = matrix->getRow(colToRow(column));
        MatrixColumn result;
        result.columnName = rowToCol(row.rowName);
        result.columnHash = rowToCol(row.rowHash);
        result.rows = colToRow(row.columns);
        return result;
    }

    virtual size_t getRowCount() const
    {
        return matrix->getColumnCount();
    }

    virtual size_t getColumnCount() const
    {
        return matrix->getRowCount();
    }

    std::pair<Date, Date>
    getTimestampRange() const
    {
        return dataset->getTimestampRange();
    }

};


/*****************************************************************************/
/* TRANSPOSED DATASET                                                        */
/*****************************************************************************/

TransposedDataset::
TransposedDataset(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress)
    : Dataset(owner)
{
    auto mergeConfig = config.params.convert<TransposedDatasetConfig>();
    
    std::shared_ptr<Dataset> dataset = obtainDataset(owner, mergeConfig.dataset,
                                                     onProgress);

    itl.reset(new Itl(server, dataset));
}

TransposedDataset::
TransposedDataset(MldbServer * owner,
                  std::shared_ptr<Dataset> dataset)
    : Dataset(owner)
{
    itl.reset(new Itl(server, dataset));
}

TransposedDataset::
~TransposedDataset()
{
}

Any
TransposedDataset::
getStatus() const
{
    return Any();
}

std::pair<Date, Date>
TransposedDataset::
getTimestampRange() const
{
    return itl->getTimestampRange();
}

std::shared_ptr<MatrixView>
TransposedDataset::
getMatrixView() const
{
    return itl;
}

std::shared_ptr<ColumnIndex>
TransposedDataset::
getColumnIndex() const
{
    return itl;
}

RegisterDatasetType<TransposedDataset, TransposedDatasetConfig> 
regTransposed(builtinPackage(),
              "transposed",
              "Dataset that interchanges rows and columns",
              "datasets/TransposedDataset.md.html");

} // namespace MLDB
} // namespace Datacratic
