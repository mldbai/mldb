// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** sub_dataset.cc                                              -*- C++ -*-
    Mathieu Marquis Bolduc, August 19th, 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

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

    addField("statement", &SubDatasetConfig::statement,
             "Select Statement that will result in the table");
}


/*****************************************************************************/
/* SUB INTERNAL REPRESENTATION                                               */
/*****************************************************************************/


struct SubDataset::Itl
    : public MatrixView, public ColumnIndex {

    std::vector<NamedRowValue> subOutput;
    std::vector<PathElement> columnNames;
    std::vector<ColumnPath> fullFlattenedColumnNames;
    Lightweight_Hash<RowHash, int64_t> rowIndex;
    Date earliest, latest;
    std::shared_ptr<ExpressionValueInfo> columnInfo;

    Itl(SelectStatement statement, MldbServer* owner, const ProgressFunc & onProgress)
    {
        SqlExpressionMldbScope mldbContext(owner);

        std::vector<NamedRowValue> rows;
        auto pair = queryFromStatementExpr(statement, mldbContext, onProgress);

        columnInfo = std::move(std::get<1>(pair));

        init(std::move(std::get<0>(pair)));
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

        // Now do a stable sort of the column names
        columnNames.insert(columnNames.end(),
                           columnNameSet.begin(), columnNameSet.end());
        std::sort(columnNames.begin(), columnNames.end());

        fullFlattenedColumnNames.insert(fullFlattenedColumnNames.end(),
                           fullflattenColumnNameSet.begin(), fullflattenColumnNameSet.end());
        std::sort(fullFlattenedColumnNames.begin(), fullFlattenedColumnNames.end());
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
            throw HttpReturnException(400, "Row '" + rowName.toUtf8String() + "' not found in sub-table dataset");
        }

        return subOutput[it->second].flatten();
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
           const std::function<bool (const Json::Value &)> & onProgress)
    : Dataset(owner)
{
    auto subConfig = config.params.convert<SubDatasetConfig>();
    
    ConvertProgressToJson convertProgressToJson(onProgress);
    itl.reset(new Itl(subConfig.statement, owner, convertProgressToJson));
}

SubDataset::
SubDataset(MldbServer * owner, SubDatasetConfig config)
    : Dataset(owner)
{
    itl.reset(new Itl(config.statement, owner, nullptr));
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
    return Any();
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
    return itl->fullFlattenedColumnNames;
}

size_t 
SubDataset::
getFlattenedColumnCount() const
{
    return itl->fullFlattenedColumnNames.size();
}

static RegisterDatasetType<SubDataset, SubDatasetConfig> 
regSub(builtinPackage(),
       "sub",
       "Dataset view on the result of a SELECT query",
       "datasets/SubDataset.md.html",
       nullptr,
       {MldbEntity::INTERNAL_ENTITY});

extern std::shared_ptr<Dataset> (*createSubDatasetFn) (MldbServer *, const SubDatasetConfig &);
extern std::shared_ptr<Dataset> (*createSubDatasetFromRowsFn) (MldbServer *, const std::vector<NamedRowValue>&);

std::shared_ptr<Dataset> createSubDataset(MldbServer * server, const SubDatasetConfig & config)
{
    return std::make_shared<SubDataset>(server, config);
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

