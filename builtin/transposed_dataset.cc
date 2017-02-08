// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** transposed_dataset.cc                                              -*- C++ -*-
    Jeremy Barnes, 28 February 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "mldb/sql/sql_expression.h"
#include "transposed_dataset.h"
#include "mldb/builtin/sub_dataset.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/structure_description.h"

using namespace std;



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
          columnCount(dataset->getFlattenedColumnCount())
    {
    }

    struct TransposedRowStream : public RowStream {

        TransposedRowStream(TransposedDataset::Itl* source) : source(source)
        {
            matrix = source->matrix;
            columns = matrix->getColumnPaths();
            column_iterator = columns.begin();
        }

        virtual std::shared_ptr<RowStream> clone() const{
            auto ptr = std::make_shared<TransposedRowStream>(source);
            return ptr;
        }

        virtual void initAt(size_t start){
            column_iterator = column_iterator + start;
        }

        virtual RowPath next() {
            return TransposedDataset::Itl::colToRow(*(column_iterator++));
        }

        virtual const RowPath & rowName(RowPath & storage) const
        {
            return storage = TransposedDataset::Itl::colToRow(*column_iterator);
        }

        virtual void advance() {
            column_iterator++;
        }

        //todo: suboptimal for large number of columns
        //need a column iter interface
        //or cache this in the transposed dataset
        vector<ColumnPath>::const_iterator column_iterator;
        vector<ColumnPath> columns;
        std::shared_ptr<MatrixView> matrix;
        TransposedDataset::Itl* source;
    };

    static RowHash colToRow(ColumnHash col)
    {
        return RowHash(col.hash());
    }

    static const RowPath & colToRow(const ColumnPath & col)
    {
        return col;
    }

    static RowPath colToRow(ColumnPath && col)
    {
        return std::move(col);
    }

    static std::vector<std::tuple<RowPath, CellValue, Date> >
    colToRow(const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals)
    {
        std::vector<std::tuple<RowPath, CellValue, Date> > result;
        result.reserve(vals.size());
        for (auto & v: vals)
            result.emplace_back(colToRow(std::get<0>(v)),
                                std::get<1>(v), std::get<2>(v));
        return result;
    }
    
    static std::vector<std::tuple<RowPath, CellValue, Date> >
    colToRow(std::vector<std::tuple<ColumnPath, CellValue, Date> > && vals)
    {
        std::vector<std::tuple<RowPath, CellValue, Date> > result;
        result.reserve(vals.size());
        for (auto & v: vals)
            result.emplace_back(colToRow(std::move(std::get<0>(v))),
                                std::move(std::get<1>(v)), std::get<2>(v));
        return result;
    }
    
    static std::vector<std::tuple<ColumnPath, CellValue, Date> >
    rowToCol(const std::vector<std::tuple<RowPath, CellValue, Date> > & vals)
    {
        std::vector<std::tuple<ColumnPath, CellValue, Date> > result;
        result.reserve(vals.size());
        for (auto & v: vals)
            result.emplace_back(rowToCol(std::get<0>(v)),
                                std::get<1>(v), std::get<2>(v));
        return result;
    }
    
    static std::vector<std::tuple<ColumnPath, CellValue, Date> >
    rowToCol(std::vector<std::tuple<RowPath, CellValue, Date> > && vals)
    {
        std::vector<std::tuple<ColumnPath, CellValue, Date> > result;
        result.reserve(vals.size());
        for (auto & v: vals)
            result.emplace_back(rowToCol(std::move(std::get<0>(v))),
                                std::move(std::get<1>(v)), std::get<2>(v));
        return result;
    }
    
    static ColumnHash rowToCol(RowHash row)
    {
        return ColumnHash(row.hash());
    }

    static const ColumnPath & rowToCol(const RowPath & row)
    {
        return row;
    }

    static ColumnPath rowToCol(RowPath && row)
    {
        return std::move(row);
    }

    virtual std::vector<RowPath>
    getRowPaths(ssize_t start = 0, ssize_t limit = -1) const
    {
        vector<ColumnPath> cols = dataset->getFlattenedColumnNames();

        vector<RowPath> result;
        for (unsigned i = start; i < cols.size();  ++i) {
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
        for (auto & n: getRowPaths()) {
            result.emplace_back(n);
        }
        return result;
    }

    virtual bool knownRow(const RowPath & rowName) const
    {
        return index->knownColumn(rowToCol(rowName));
    }

    virtual bool knownColumn(const ColumnPath & columnName) const
    {
        return matrix->knownRow(colToRow(columnName));
    }

    virtual RowPath getRowPath(const RowHash & row) const
    {
        return matrix->getColumnPath(rowToCol(row));
    }

    virtual ColumnPath getColumnPath(ColumnHash column) const
    {
        return matrix->getRowPath(colToRow(column));
    }

    virtual MatrixNamedRow getRow(const RowPath & rowName) const
    {
        MatrixColumn col = index->getColumn(rowToCol(rowName));
        MatrixNamedRow result;
        result.rowName = colToRow(std::move(col.columnName));
        result.rowHash = colToRow(col.columnHash);
        result.columns = rowToCol(std::move(col.rows));
        return result;
    }

    virtual ExpressionValue getRowExpr(const RowPath & rowName) const
    {
        MatrixColumn col = index->getColumn(rowToCol(rowName));
        return std::move(col.rows);
    }

    virtual std::vector<ColumnPath> getColumnPaths() const
    {
        std::vector<ColumnPath> result;
        result.reserve(matrix->getColumnCount());

        for (auto & c: matrix->getRowPaths())
            result.emplace_back(rowToCol(c));
        
        return result;
    }

    virtual const ColumnStats &
    getColumnStats(const ColumnPath & columnName, ColumnStats & stats) const
    {
        auto row = matrix->getRow(colToRow(columnName));

        stats = ColumnStats();

        Lightweight_Hash_Set<ColumnHash> columns;
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

    virtual uint64_t getColumnRowCount(const ColumnPath & column) const
    {
        return matrix->getRowColumnCount(colToRow(column));
    }

    virtual uint64_t getRowColumnCount(const RowPath & row) const
    {
        return index->getColumnRowCount(rowToCol(row));
    }

    /** Return the value of the column for all rows and timestamps. */
    virtual MatrixColumn getColumn(const ColumnPath & column) const
    {
        auto row = matrix->getRow(colToRow(column));
        MatrixColumn result;
        result.columnName = rowToCol(row.rowName);
        result.columnHash = rowToCol(row.rowHash);
        result.rows = colToRow(std::move(row.columns));
        return result;
    }

    virtual size_t getRowCount() const
    {
        return dataset->getFlattenedColumnCount();
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
                  const ProgressFunc & onProgress)
    : Dataset(owner)
{
    auto mergeConfig = config.params.convert<TransposedDatasetConfig>();
    
    std::shared_ptr<Dataset> dataset = obtainDataset(owner, mergeConfig.dataset,
                                                     nullptr /*onProgress*/);

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

std::shared_ptr<RowStream> 
TransposedDataset::
getRowStream() const
{
    return make_shared<TransposedDataset::Itl::TransposedRowStream>(itl.get());
}

ExpressionValue
TransposedDataset::
getRowExpr(const RowPath & row) const
{
    return itl->getRowExpr(row);
}

RegisterDatasetType<TransposedDataset, TransposedDatasetConfig> 
regTransposed(builtinPackage(),
              "transposed",
              "Dataset that interchanges rows and columns",
              "datasets/TransposedDataset.md.html");

extern std::shared_ptr<Dataset> (*createTransposedDatasetFn) (MldbServer *, std::shared_ptr<Dataset> dataset);
extern std::shared_ptr<Dataset> (*createTransposedTableFn) (MldbServer *, const TableOperations& table);

std::shared_ptr<Dataset> createTransposedDataset(MldbServer * server, std::shared_ptr<Dataset> dataset)
{  
    return std::make_shared<TransposedDataset>(server, dataset);
}

std::shared_ptr<Dataset> createTransposedTable(MldbServer * server, const TableOperations& table)
{
    SqlBindingScope dummyScope;
    auto generator = table.runQuery(dummyScope,
                                   SelectExpression::STAR,
                                   WhenExpression::TRUE,
                                   *SqlExpression::TRUE,
                                   OrderByExpression(),
                                    0, -1,
                                    nullptr /*onProgress*/);

    SqlRowScope fakeRowScope;

    // Generate all outputs of the query
    std::vector<NamedRowValue> rows
        = generator(-1, fakeRowScope);

    auto subDataset = std::make_shared<SubDataset>(server, rows);

    return std::make_shared<TransposedDataset>(server, subDataset);
}

namespace {
struct AtInit {
    AtInit()
    {
        createTransposedDatasetFn = createTransposedDataset;
        createTransposedTableFn = createTransposedTable;
    }
} atInit;
}

} // namespace MLDB

