/** randomforest_procedure.h                                            -*- C++ -*-
    Mathieu Marquis Bolduc, 11 Mars 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    Scope to bind expression using dataset known columns
*/


#pragma once

#include "mldb/server/dataset_context.h"
#include "mldb/base/parallel.h"
#include "mldb/base/thread_pool.h"
#include "mldb/http/http_exception.h"
#include "mldb/core/dataset.h"

namespace Datacratic {
namespace MLDB {

struct ColumnScope: public SqlExpressionMldbScope {
    ColumnScope(MldbServer * server, std::shared_ptr<Dataset> dataset)
        : SqlExpressionMldbScope(server), dataset(dataset)
    {
    }

    std::shared_ptr<Dataset> dataset;

    std::map<ColumnName, size_t> requiredColumnIndexes;
    std::vector<ColumnName> requiredColumns;

    struct RowScope: public SqlRowScope {
        RowScope(size_t rowIndex,
                 const std::vector<std::vector<CellValue> > & inputs)
            : rowIndex(rowIndex), inputs(inputs)
        {
        }

        size_t rowIndex;
        const std::vector<std::vector<CellValue> > & inputs;
    };

    virtual ColumnGetter
    doGetColumn(const Utf8String & tableName,
                const ColumnName & columnName)
    {
        if (!requiredColumnIndexes.count(columnName)) {
            size_t index = requiredColumns.size();
            requiredColumnIndexes[columnName] = index;
            requiredColumns.push_back(columnName);
        }

        size_t index = requiredColumnIndexes[columnName];
        
        return {[=] (const SqlRowScope & scope,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    auto & row = scope.as<RowScope>();
                    return storage
                        = ExpressionValue(row.inputs.at(index).at(row.rowIndex),
                                          Date::notADate());
                },
                std::make_shared<AtomValueInfo>()};
    }

    virtual GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    std::function<ColumnName (const ColumnName &)> keep)
    {
        throw HttpReturnException(400, "Attempt to bind expression with wildcard in column scope");
    }

    virtual BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope)
    {
        return SqlBindingScope::doGetFunction(tableName, functionName, args, argScope);
    }

    std::vector<std::vector<CellValue> >
    run(const std::vector<BoundSqlExpression> & exprs) const
    {
        size_t numRows = dataset->getMatrixView()->getRowCount();

        std::vector<std::vector<CellValue> > inputs(requiredColumns.size());
        for (size_t i = 0;  i < inputs.size();  ++i) {
            inputs[i] = dataset->getColumnIndex()
                ->getColumnDense(requiredColumns[i]);
        }

        std::vector<std::vector<CellValue> > results(exprs.size());
        for (auto & r: results)
            r.resize(numRows);

        // Apply the expression to everything
        auto doRow = [&] (size_t first, size_t last)
            {
                for (size_t i = first;  i < last;  ++i) {
                    RowScope scope(i, inputs);
                    for (unsigned j = 0;  j < exprs.size();  ++j) {
                        ExpressionValue storage;
                        const ExpressionValue & result
                            = exprs[j](scope, storage, GET_LATEST);
                
                        // Currently, only atoms are supported as results
                        results[j][i] = result.getAtom();
                    }
                }
            };
        
        parallelMapChunked(0, numRows, 1024 /* rows at once */,
                           doRow);

        return std::move(results);
    }
};

}
}
