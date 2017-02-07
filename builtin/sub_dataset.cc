// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** sub_dataset.cc                                              -*- C++ -*-
    Mathieu Marquis Bolduc, August 19th, 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "mldb/sql/sql_expression.h"
#include "mldb/server/dataset_context.h"
#include "mldb/server/analytics.h"
#include "sub_dataset.h"
#include "mldb/types/any_impl.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/types/structure_description.h"
#include "mldb/http/http_exception.h"
#include <unordered_set>
#include <mutex>

using namespace std;



namespace MLDB {


/*****************************************************************************/
/* SUB DATASET CONFIG                                                     */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(SubDatasetConfig);

SubDatasetConfigDescription::
SubDatasetConfigDescription()
{
    nullAccepted = true;
}


/*****************************************************************************/
/* SUB INTERNAL REPRESENTATION                                               */
/*****************************************************************************/


struct SubDataset::Itl
    : public MatrixView, public ColumnIndex {

    std::vector<NamedRowValue> subOutput;
    std::set<PathElement> columnNames;
    std::set<ColumnPath> fullFlattenedColumnNames;
    Lightweight_Hash<RowHash, int64_t> rowIndex;
    Date earliest, latest;
    std::shared_ptr<ExpressionValueInfo> columnInfo;
    std::mutex recordLock;

    Itl(SelectStatement statement, MldbServer* owner, const ProgressFunc & onProgress)
    {
        if (statement.from) {
             SqlExpressionMldbScope mldbContext(owner);

            auto pair = queryFromStatementExpr(statement, mldbContext, nullptr /*params*/, onProgress);

            columnInfo = std::move(std::get<1>(pair));

            init(std::move(std::get<0>(pair)));
        }       
    }

    Itl(std::vector<NamedRowValue> rows)
    {
        init(std::move(rows));
    }

    void init(std::vector<NamedRowValue> rows)
    {
        this->subOutput = std::move(rows);

        earliest = latest = Date::notADate();

        std::unordered_set<PathElement> columnNameSet;
        std::unordered_set<ColumnPath> fullflattenColumnNameSet;
        bool first = true;

        // Scan all rows for the columns that are there
        
        for (size_t i = 0;  i < subOutput.size();  ++i) {
            const NamedRowValue & row = subOutput[i];

            ExcAssert(row.rowName != RowPath());

            rowIndex[row.rowName] = i;

            for (auto& c : row.columns)
            {
                const PathElement & cName = std::get<0>(c);               

                columnNameSet.insert(cName);

                Date ts = std::get<1>(c).getEffectiveTimestamp();
                
                if (ts.isADate()) {
                    if (first) {
                        earliest = latest = ts;
                        first = false;
                    }
                    else {
                        earliest.setMin(ts);
                        latest.setMax(ts);
                    }
                }

                auto getName = [&] (const Path & columnName,
                                   const Path & prefix,
                                   const CellValue & val,
                                   Date ts) -> bool
                {
                    fullflattenColumnNameSet.insert(prefix + columnName);
                    return true;
                };

                std::get<1>(c).forEachAtom(getName, cName);
            }
        }

        columnNames.insert(columnNameSet.begin(), columnNameSet.end());

        fullFlattenedColumnNames.insert(fullflattenColumnNameSet.begin(), 
            fullflattenColumnNameSet.end());

    }

    void AddRowInternal(const RowPath & rowName,
                const ExpressionValue & expr)
    {
        NamedRowValue newRow;
        newRow.columns.reserve(expr.rowLength());
        auto onSubexpr = [&] (const PathElement & columnName,
                              const ExpressionValue & val)
            {
                newRow.columns.emplace_back(std::move(columnName), val);
                return true;
            };

        expr.forEachColumn(onSubexpr);
        newRow.rowName = rowName;
        newRow.rowHash = rowName;
        {
            std::lock_guard<std::mutex> lock(recordLock);
            this->subOutput.push_back(std::move(newRow));
        }

        std::unordered_set<PathElement> columnNameSet;
        std::unordered_set<ColumnPath> fullflattenColumnNameSet;
        bool first = earliest == Date::notADate() && latest == Date::notADate();

        const NamedRowValue & row = subOutput.back();
        ExcAssert(row.rowName != RowPath());
        {
            std::lock_guard<std::mutex> lock(recordLock);
                        
            if (!rowIndex.insert({row.rowHash, this->subOutput.size() - 1}).second) {
                throw HttpReturnException
                    (400, "Duplicate row name in dataset",
                     "rowName",
                     row.rowName);
            }
        }

        for (auto& c : row.columns)
        {
            const PathElement & cName = std::get<0>(c);

            if (std::find(columnNameSet.begin(), columnNameSet.end(), cName) == columnNameSet.end()
                && std::find(columnNames.begin(), columnNames.end(), cName) == columnNames.end()) {
                columnNameSet.insert(cName);
            }

            Date ts = std::get<1>(c).getEffectiveTimestamp();
            
            if (ts.isADate()) {
                if (first) {
                    earliest = latest = ts;
                    first = false;
                }
                else {
                    earliest.setMin(ts);
                    latest.setMax(ts);
                }
            }

            auto getName = [&] (const Path & columnName,
                               const Path & prefix,
                               const CellValue & val,
                               Date ts) -> bool
            {
                fullflattenColumnNameSet.insert(prefix + columnName);
                return true;
            };

            std::get<1>(c).forEachAtom(getName, cName);
        }

        {
            std::lock_guard<std::mutex> lock(recordLock);
            columnNames.insert(columnNameSet.begin(), columnNameSet.end());
            fullFlattenedColumnNames.insert(fullflattenColumnNameSet.begin(), 
                fullflattenColumnNameSet.end());
        }
        
    }

    void AddRow(const RowPath & rowName,
                const ExpressionValue & expr)
    {
        AddRowInternal(rowName, expr);
    }

    void AddRows(const std::vector<std::pair<RowPath, ExpressionValue> > & rows)
    {
        for (auto& p : rows) {
            AddRow(p.first, p.second);
        }
    }

    void
    AddRowNonStructured(const RowPath & rowName,
                        const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals)
    {
        AddRow(rowName, ExpressionValue(vals));
    }

    ~Itl() { }

    struct SubRowStream : public RowStream {

        SubRowStream(SubDataset::Itl* source) : source(source)
        {
            
        }

        virtual std::shared_ptr<RowStream> clone() const{
            auto ptr = std::make_shared<SubRowStream>(source);
            return ptr;
        }

        virtual void initAt(size_t start){
            iter = source->subOutput.begin() + start;
        }

        virtual RowPath next() {
            return (iter++)->rowName;
        }

        virtual const RowPath & rowName(RowPath & storage) const
        {
            return iter->rowName;
        }

        std::vector<NamedRowValue>::const_iterator iter;
        SubDataset::Itl* source;
    };


    virtual std::vector<RowPath>
    getRowPaths(ssize_t start = 0, ssize_t limit = -1) const
    {    
        std::vector<RowPath> result;
        
        for (size_t index = start;
             index < subOutput.size() && (limit == -1 || index < start + limit);
             ++index) {
            result.push_back(subOutput[index].rowName);
        };

        return result;
    }

    virtual std::vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const
    {
        std::vector<RowHash> result;
        
        for (size_t index = start;
             index < subOutput.size() && (limit == -1 || index < start + limit);
             ++index) {
            result.push_back(subOutput[index].rowName);
        };

        return result;
    }

    virtual bool knownRow(const RowPath & rowName) const
    {
        return rowIndex.count(rowName);
    }

    virtual MatrixNamedRow getRow(const RowPath & rowName) const
    {
        auto it = rowIndex.find(rowName);
        if (it == rowIndex.end()) {
            throw HttpReturnException(400, "Row '" + rowName.toUtf8String() + "' not found in dataset");
        }

        return subOutput[it->second].flatten();
    }

    ExpressionValue
    getRowExpr(const RowPath & rowName) const
    {

        auto it = rowIndex.find(rowName);
        if (it == rowIndex.end()) {
            throw HttpReturnException(400, "Row '" + rowName.toUtf8String() + "' not found in dataset");
        }

        return subOutput[it->second].columns;
    }

    virtual RowPath getRowPath(const RowHash & rowHash) const
    {
        auto it = rowIndex.find(rowHash);
        if (it == rowIndex.end()) {
            throw HttpReturnException(400, "Row not found in sub-table dataset");
        }

        return subOutput[it->second].rowName;
    }

    virtual bool knownColumn(const ColumnPath & column) const
    {
        if (column.size() == 1)
            return std::find(columnNames.begin(), columnNames.end(), column[0]) != columnNames.end();
        else
            return std::find(fullFlattenedColumnNames.begin(), fullFlattenedColumnNames.end(), column) != fullFlattenedColumnNames.end();
    }

    virtual ColumnPath getColumnPath(ColumnHash columnHash) const
    {        
        for (const auto& c : columnNames)
        {
            if (ColumnHash(ColumnPath(c)) == columnHash)
            {
                return c;
            }
        }

        return ColumnPath();
    }

    /** Return a list of all columns. */
    virtual std::vector<ColumnPath> getColumnPaths() const
    {
        std::vector<ColumnPath> fullColumnNames;
        fullColumnNames.reserve(columnNames.size());
        for (const auto& c : columnNames)
        {
            fullColumnNames.push_back(ColumnPath(c));
        }

        return fullColumnNames;
    }

    /** Return the value of the column for all rows and timestamps. */
    virtual MatrixColumn getColumn(const ColumnPath & columnName) const
    {
        MatrixColumn output;
        output.columnHash = columnName;
        output.columnName = columnName;

        for (const auto& row : subOutput)
        {            
            auto flattened = row.flatten();

            for (const auto& c : flattened.columns)
            {
                const ColumnPath & cName = std::get<0>(c);

                if (cName == columnName)
                {
                    output.rows.emplace_back(row.rowName, std::get<1>(c), std::get<2>(c));
                }
            }
        }

        return output;
    }

    /** Return the value of the column for all rows and timestamps. */
    virtual std::vector<std::tuple<RowPath, CellValue> >
    getColumnValues(const ColumnPath & columnName,
                    const std::function<bool (const CellValue &)> & filter) const
    {
        std::vector<std::tuple<RowPath, CellValue> > result; 

        for (const auto& row : subOutput)
        {
            auto flattened = row.flatten();

            for (const auto& c : flattened.columns)
            {
                const ColumnPath & cName = std::get<0>(c);

                if (cName == columnName)
                {
                    const CellValue & cell = std::get<1>(c);
                    if (!filter || filter(cell))
                    {
                        result.emplace_back(row.rowName, cell);
                    }
                }
            }
        }

        return result;
    }

    virtual size_t getRowCount() const
    {
        return subOutput.size();
    }

    virtual size_t getColumnCount() const
    {
        return columnNames.size();
    }   

    std::pair<Date, Date>
    getTimestampRange() const
    {
        return { earliest, latest };
    }

};


/*****************************************************************************/
/* SUB DATASET                                                               */
/*****************************************************************************/

SubDataset::
SubDataset(MldbServer * owner,
           PolyConfig config,
           const ProgressFunc & onProgress)
    : Dataset(owner)
{
    auto subConfig = config.params.convert<SubDatasetConfig>();
    
    itl.reset(new Itl(subConfig.statement, owner, onProgress));
}

SubDataset::
SubDataset(MldbServer * owner, SubDatasetConfig config, const ProgressFunc & onProgress)
    : Dataset(owner)
{
    itl.reset(new Itl(config.statement, owner, onProgress));
}

SubDataset::
SubDataset(MldbServer * owner, std::vector<NamedRowValue> rows)
    : Dataset(owner)
{
    itl.reset(new Itl(std::move(rows)));
}

SubDataset::
~SubDataset()
{

}

Any
SubDataset::
getStatus() const
{
    Json::Value status;
    status["rowCount"] = itl->subOutput.size();
    status["columnCount"] = itl->fullFlattenedColumnNames.size();

    return status;
}

std::pair<Date, Date>
SubDataset::
getTimestampRange() const
{
    return itl->getTimestampRange();
}

std::shared_ptr<MatrixView>
SubDataset::
getMatrixView() const
{
    return itl;
}

std::shared_ptr<ColumnIndex>
SubDataset::
getColumnIndex() const
{
    return itl;
}

void
SubDataset::
recordRowExpr(const RowPath & rowName,
              const ExpressionValue & expr)
{
    ExcAssert(itl);
    itl->AddRows({{rowName, expr}});
}

void
SubDataset::
recordRowsExpr(const std::vector<std::pair<RowPath, ExpressionValue> > & rows) {
     ExcAssert(itl);
     itl->AddRows(rows);
}

void
SubDataset::
recordRowItl(const RowPath & rowName,
             const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals)
{
    ExcAssert(itl);
    itl->AddRowNonStructured(rowName, vals);
}

ExpressionValue
SubDataset::
getRowExpr(const RowPath & row) const
{
   return itl->getRowExpr(row);
}

std::shared_ptr<RowStream> 
SubDataset::
getRowStream() const
{
    return make_shared<SubDataset::Itl::SubRowStream>(itl.get());
}

KnownColumn
SubDataset::
getKnownColumnInfo(const ColumnPath & columnName) const
{
    if (itl->columnInfo) {
        std::shared_ptr<ExpressionValueInfo> columnInfo = itl->columnInfo->findNestedColumn(columnName);
        if (columnInfo)
            return KnownColumn(columnName, columnInfo, COLUMN_IS_SPARSE);
    }

    return Dataset::getKnownColumnInfo(columnName);
}

std::vector<ColumnPath> 
SubDataset::
getFlattenedColumnNames() const
{
    return std::vector<ColumnPath>(itl->fullFlattenedColumnNames.begin(), itl->fullFlattenedColumnNames.end());
}

size_t 
SubDataset::
getFlattenedColumnCount() const
{
    return itl->fullFlattenedColumnNames.size();
}

static RegisterDatasetType<SubDataset, SubDatasetConfig> 
regSub(builtinPackage(),
       "structured.mutable",
       "Dataset optimized for structured data",
       "datasets/StructuredDataset.md.html",
       nullptr,
       {MldbEntity::INTERNAL_ENTITY});

extern std::shared_ptr<Dataset> (*createSubDatasetFn) (MldbServer *, 
                                                       const SubDatasetConfig &, 
                                                       const ProgressFunc &);
extern std::shared_ptr<Dataset> (*createSubDatasetFromRowsFn) (MldbServer *, const std::vector<NamedRowValue>&);

std::shared_ptr<Dataset> createSubDataset(MldbServer * server, 
                                          const SubDatasetConfig & config, 
                                          const ProgressFunc & onProgress)
{
    return std::make_shared<SubDataset>(server, config, onProgress);
}

std::shared_ptr<Dataset> createSubDatasetFromRows(MldbServer * server, const std::vector<NamedRowValue>& rows)
{
    return std::make_shared<SubDataset>(server, rows);
}


std::vector<NamedRowValue>
querySubDataset(MldbServer * server,
                std::vector<NamedRowValue> rows,
                const SelectExpression & select,
                const WhenExpression & when,
                const SqlExpression & where,
                const OrderByExpression & orderBy,
                const TupleExpression & groupBy,
                const std::shared_ptr<SqlExpression> having,
                const std::shared_ptr<SqlExpression> named,
                uint64_t offset,
                int64_t limit,
                const Utf8String & tableAlias,
                bool allowMultiThreading)
{
    auto dataset = std::make_shared<SubDataset>
        (server, std::move(rows));
                
    std::vector<MatrixNamedRow> output
        = dataset
        ->queryStructured(select, when, where, orderBy, groupBy,
                          having, named, offset, limit, "" /* alias */);
    
    std::vector<NamedRowValue> result;
    result.reserve(output.size());
                
    for (auto & row: output) {
        // All of this is to properly unflatten the output of the
        // queryStructured call.
        ExpressionValue val(std::move(row.columns));
        NamedRowValue rowOut;
        rowOut.rowName = std::move(row.rowName);
        rowOut.rowHash = std::move(row.rowHash);
        val.mergeToRowDestructive(rowOut.columns);
        result.emplace_back(std::move(rowOut));
    }

    return result;
}

// Overridden by libmldb.so when it loads up to break circular link dependency
// and allow expression parsing to be in a separate library
extern std::vector<NamedRowValue>
(*querySubDatasetFn) (MldbServer * server,
                      std::vector<NamedRowValue> rows,
                      const SelectExpression & select,
                      const WhenExpression & when,
                      const SqlExpression & where,
                      const OrderByExpression & orderBy,
                      const TupleExpression & groupBy,
                      const std::shared_ptr<SqlExpression> having,
                      const std::shared_ptr<SqlExpression> named,
                      uint64_t offset,
                      int64_t limit,
                      const Utf8String & tableAlias,
                      bool allowMultiThreading);


namespace {
struct AtInit {
    AtInit()
    {
        createSubDatasetFn = createSubDataset;
        createSubDatasetFromRowsFn = createSubDatasetFromRows;
        querySubDatasetFn = querySubDataset;
    }
} atInit;

} // file scope

} // namespace MLDB

