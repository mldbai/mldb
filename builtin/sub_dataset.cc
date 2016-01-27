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


namespace Datacratic {
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
/* SUB INTERNAL REPRESENTATION                                            */
/*****************************************************************************/


struct SubDataset::Itl
    : public MatrixView, public ColumnIndex {

    std::vector<MatrixNamedRow> subOutput;
    std::vector<ColumnName> columnNames;
    ML::Lightweight_Hash<RowHash, int64_t> rowIndex;
    Date earliest, latest;

    Itl(SelectStatement statement, MldbServer* owner)
    {
        SqlExpressionMldbContext mldbContext(owner);
        BoundTableExpression table = statement.from->bind(mldbContext);  

        if (table.dataset)
        {  
            subOutput = table.dataset->queryStructured(statement.select, statement.when, 
                                                  *statement.where,
                                                  statement.orderBy,
                                                  statement.groupBy,
                                                  *statement.having,
                                                  *statement.rowName,
                                                  statement.offset,
                                                  statement.limit,
                                                  table.asName);
        }
        else
        {
            subOutput = queryWithoutDataset(statement, mldbContext);
        }

        earliest = latest = Date::notADate();

        std::unordered_set<ColumnName> columnNameSet;
        bool first = true;

        // Scan all rows for the columns that are there
        
        for (size_t i = 0;  i < subOutput.size();  ++i) {
            const MatrixNamedRow & row = subOutput[i];

            rowIndex[row.rowName] = i;

            for (auto& c : row.columns)
            {
                const ColumnName & cName = std::get<0>(c);
                const Date & ts = std::get<2>(c);

                columnNameSet.insert(cName);
                
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
            }
        }

        // Now do a stable sort of the column names
        columnNames.insert(columnNames.end(),
                           columnNameSet.begin(), columnNameSet.end());
        std::sort(columnNames.begin(), columnNames.end());
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

        virtual RowName next() {
            return (iter++)->rowName;
        }

        std::vector<MatrixNamedRow>::const_iterator iter;
        SubDataset::Itl* source;
    };


    virtual std::vector<RowName>
    getRowNames(ssize_t start = 0, ssize_t limit = -1) const
    {    
        std::vector<RowName> result;
        
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

    virtual bool knownRow(const RowName & rowName) const
    {
        return rowIndex.count(rowName);
    }

    virtual MatrixNamedRow getRow(const RowName & rowName) const
    {
        auto it = rowIndex.find(rowName);
        if (it == rowIndex.end()) {
            throw HttpReturnException(400, "Row not found in sub-table dataset");
        }

        return subOutput[it->second];
    }

    virtual RowName getRowName(const RowHash & rowHash) const
    {
        auto it = rowIndex.find(rowHash);
        if (it == rowIndex.end()) {
            throw HttpReturnException(400, "Row not found in sub-table dataset");
        }

        return subOutput[it->second].rowName;
    }

    virtual bool knownColumn(const ColumnName & column) const
    {
        return std::find(columnNames.begin(), columnNames.end(), column) != columnNames.end();
    }

    virtual ColumnName getColumnName(ColumnHash columnHash) const
    {        
        for (auto& c : columnNames)
        {
            if (ColumnHash(c) == columnHash)
            {
                return c;
            }
        }

        return ColumnName();
    }

    /** Return a list of all columns. */
    virtual std::vector<ColumnName> getColumnNames() const
    {
        return columnNames;
    }   

    /** Return the value of the column for all rows and timestamps. */
    virtual MatrixColumn getColumn(const ColumnName & columnName) const
    {
        MatrixColumn output;
        output.columnHash = columnName;
        output.columnName = columnName;

        for (auto row : subOutput)
        {            
            for (auto c : row.columns)
            {
                const ColumnName & cName = std::get<0>(c);

                if (cName == columnName)
                {
                    output.rows.emplace_back(row.rowName, std::get<1>(c), std::get<2>(c));
                }
            }
        }

        return output;
    }

    /** Return the value of the column for all rows and timestamps. */
    virtual std::vector<std::tuple<RowName, CellValue> >
    getColumnValues(const ColumnName & columnName,
                    const std::function<bool (const CellValue &)> & filter) const
    {
        std::vector<std::tuple<RowName, CellValue> > result; 

        for (auto row : subOutput)
        {
            for (auto c : row.columns)
            {
                const ColumnName & cName = std::get<0>(c);

                if (cName == columnName)
                {
                    const CellValue & cell = std::get<1>(c);
                    if (filter(cell))
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
    
    itl.reset(new Itl(subConfig.statement, owner));
}

SubDataset::
SubDataset(MldbServer * owner, SubDatasetConfig config)
    : Dataset(owner)
{
    itl.reset(new Itl(config.statement, owner));
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

static RegisterDatasetType<SubDataset, SubDatasetConfig> 
regSub(builtinPackage(),
          "sub",
          "Dataset view on the result of a SELECT query",
          "datasets/SubDataset.md.html");

extern std::shared_ptr<Dataset> (*createSubDatasetFn) (MldbServer *, const SubDatasetConfig &);

std::shared_ptr<Dataset> createSubDataset(MldbServer * server, const SubDatasetConfig & config)
{
    return std::make_shared<SubDataset>(server, config);
}

namespace {
struct AtInit {
    AtInit()
    {
        createSubDatasetFn = createSubDataset;
    }
} atInit;

} // file scope

} // namespace MLDB
} // namespace Datacratic
