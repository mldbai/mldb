/** decompose.cc                                               -*- C++ -*-
    Jeremy Barnes, 24 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Base SQL expression support.
*/

#include "decompose.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/types/value_description.h"
#include "mldb/base/optimized_path.h"


using namespace std;


namespace MLDB {

namespace {
// Allow the move into place optimization to be turned on or off to aid
// in unit esting.

static OptimizedPath moveIntoOutputs("mldb.sql.decompose.moveIntoOutputs");
} // namespace 

Decomposition decompose(const BoundSqlExpression & selectBound,
                        SqlBindingScope & scope)
{
    vector<ColumnPath> knownColumnNames;
    LightweightHash<ColumnHash, int> inputColumnIndex;
    LightweightHash<ColumnHash, int> columnIndex; //To check for duplicates column names

    auto cols = selectBound.info->getKnownColumns();

    for (unsigned i = 0;  i < cols.size();  ++i) {
        const auto& col = cols[i];
        if (!col.valueInfo->isScalar())
            throw AnnotatedException
                (400,
                "Row-valued select expression cannot be decomposed.");

        ColumnHash ch(col.columnName);
        if (!columnIndex.insert(make_pair(ch, i)).second)
            throw AnnotatedException(400, "Duplicate column name in select expression",
                                    "columnName", col.columnName);

        knownColumnNames.emplace_back(col.columnName);
    }

    if (!selectBound.decomposition) {
        cerr << "no decomposition for selectBound" << endl;
        return {};
    }

    // Which is the last clause using each input?
    std::map<Path, int> lastClauseUsingInput;
    std::vector<BoundSqlExpression> otherClauses;
    std::vector<ColumnOperation> ops;
    bool canUseDecomposed = true;
    
    for (size_t i = 0;  i < selectBound.decomposition->size();  ++i) {
        auto & d = (*selectBound.decomposition)[i];

        // What can be optimized?
        // select input_column [as output_name] --> copy or move in place
        // select expr(input_column) [ as output_name ] --> run destructive
        
        //cerr << "scanning clause " << i << ": " << jsonEncode(d) << endl;
        
        // Only single input to single output clauses can be run
        // in place like this
        // TODO: later, we can expand to multiple-input clauses...
        if (d.inputs.size() != 1 || d.inputWildcards.size() != 0
            || d.outputs.size() != 1 || d.outputWildcards) {
            //cerr << "    clause fails" << endl;
            canUseDecomposed = false;
            otherClauses.emplace_back(d.expr->bind(scope));
            continue;
        }

        // Operation to execute to run this column
        ColumnOperation exec;
        exec.clauseNum = i;
        
        //cerr << "  clause passes" << endl;

        // Record whether we're the last input or not
        for (const Path & input: d.inputs) {
            lastClauseUsingInput[input] = i;
        }
        
        ColumnPath inputName = *d.inputs.begin();
        ColumnPath outputName = d.outputs[0];

        auto it = inputColumnIndex.find(inputName);
    
        if (it == inputColumnIndex.end()) {
            otherClauses.emplace_back(d.expr->bind(scope));
            continue;
        }

        int inputIndex = it->second;

        it = columnIndex.find(outputName);
        if (it == columnIndex.end())
            throw AnnotatedException(500, "Output column name not found");
    
        int outputIndex = it->second;

        exec.inputCols.emplace_back(inputIndex);
        exec.outCol = outputIndex;

        if (d.expr->getType() == "selectExpr") {
            // simple copy
            auto op
                = static_cast<const NamedColumnExpression *>(d.expr.get());

            // Simply reading a variable can just make a copy.  Later
            // we see if we can move instead of copy.
            if (op->expression->getType() == "variable") {
                const ColumnPath & varName
                    = static_cast<const ReadColumnExpression *>
                    (op->expression.get())->columnName;

                ExcAssertEqual(varName, inputName);
                
                //cerr << "op " << ops.size() << ": copy "
                //     << inputName << " at " << inputIndex
                //     << " to " << outputName << " at " << outputIndex
                //     << endl;
                ops.emplace_back(std::move(exec));
                continue;
            }
            
            // Bind a much simpler value
            exec.bound = op->expression->bind(scope);
        
            //cerr << "op " << ops.size() << ": compute "
            //     << outputName << " at " << outputIndex
            //     << " from " << inputName << " with "
            //     << op->expression->print() << endl;

            ops.emplace_back(std::move(exec));
            continue;
        }

        //cerr << "*** not a select ***" << endl;
    
        //cerr << "warning: operation " << d.expr->print() << " unhandled"
        //     << endl;

        otherClauses.emplace_back(d.expr->bind(scope));
        canUseDecomposed = false;
        ExcAssert(false);
    }

    if (moveIntoOutputs.take()) {
        //cerr << "move into outputs" << endl;
        // Analyze if we're the last operation using a given input.
        // If so, we can move our inputs to our outputs instead of
        // copying them.   This is a pure optimization.
        for (auto & op: ops) {
            int clauseNum = op.clauseNum;
            const auto & d = (*selectBound.decomposition)[clauseNum];
            op.moveInputs = true;
            for (const Path & input: d.inputs) {
                int lastUsed = lastClauseUsingInput[input];
                if (lastUsed > clauseNum) {
                    op.moveInputs = false;
                    break;
                }
            }

            //if (op.moveInputs) {
            //    cerr << "turning clause " << clauseNum
            //         << " from copy into move" << endl;
            //}
        }
    }

    return {
        std::move(otherClauses),
        std::move(ops),
        std::move(knownColumnNames),
        std::move(inputColumnIndex),
        std::move(columnIndex),
        canUseDecomposed
    };
};

std::tuple<std::vector<CellValue>, std::vector<std::pair<ColumnPath, CellValue>>>
Decomposition::apply(SqlRowScope & row,
                     std::span<const CellValue> values) const
{
    std::vector<CellValue> outputValues(knownColumnNames.size());
    std::vector<bool> outputValueSet(knownColumnNames.size(), false);

    // Apply the operations one by one
    for (size_t i = 0;  i < ops.size();  ++i) {
        const ColumnOperation & op = ops[i];

        // Process this input
        if (op.bound) {
            ExpressionValue opStorage;
            const ExpressionValue & newVal
                = op.bound(row, opStorage, GET_ALL);

            // We record the output if it is used.  This would only
            // not be true for operations that have no output but
            // do cause side effects.
            if (op.outCol != -1) {
                if (&newVal == &opStorage)
                    outputValues[op.outCol] = opStorage.stealAtom();
                else outputValues[op.outCol] = newVal.getAtom();
                outputValueSet[op.outCol] = true;
            }
        }
        else {
            ExcAssertEqual(op.inputCols.size(), 1);
            int inCol = op.inputCols[0];

            // Copy or move value directly from input to output
            if (op.moveInputs) {
                outputValues[op.outCol] = std::move(values[inCol]);
            }
            else {
                outputValues[op.outCol] = values[inCol];
            }
            outputValueSet[op.outCol] = true;
        }
    }

    // Extra values we couldn't analyze statically, which will
    // be recorded when one of the columns has extra values
    std::vector<std::pair<ColumnPath, CellValue> > extra;
    
    for (auto & clause: otherClauses) {
        ExpressionValue clauseStorage;
        const ExpressionValue & clauseOutput
            = clause(row, clauseStorage, GET_ALL);

        // This must be a row output, since it's going to be
        // merged together
        if (&clauseOutput == &clauseStorage) {
            auto recordAtom = [&] (Path & columnName,
                                CellValue & value,
                                Date ts) -> bool
                {
                    // Is it the first time we've set this column?
                    auto it = columnIndex.find(columnName);
                    if (it != columnIndex.end()
                        && !outputValueSet.at(it->second)) {
                        // If so, put it in output values
                        outputValues[it->second] = std::move(value);
                        outputValueSet[it->second] = true;
                    }
                    else {
                        // If not, put it in the extra values
                        extra.emplace_back(std::move(columnName),
                                        std::move(value));
                    }
                    return true;
                };
            
            clauseStorage.forEachAtomDestructive(recordAtom);
        }
        else {
            // We don't own the output, so we need to copy
            // things before we record them.
            
            auto recordAtom = [&] (const Path & columnName_,
                                const Path & prefix,
                                const CellValue & value,
                                Date ts) -> bool
                {
                    Path columnName2;
                    if (!prefix.empty()) {
                        columnName2 = prefix + columnName_;
                    }
                    const Path & columnName
                        = prefix.empty()
                        ? columnName_
                        : columnName2;
                    
                    // Is it the first time we've set this column?
                    auto it = columnIndex.find(columnName);
                    if (it != columnIndex.end()
                        && !outputValueSet.at(it->second)) {
                        // If so, put it in output values
                        outputValues[it->second] = value;
                        outputValueSet[it->second] = true;
                    }
                    else {
                        // If not, put it in the extra values
                        if (prefix.empty()) {
                            extra.emplace_back(std::move(columnName2),
                                            value);
                        }
                        else {
                            extra.emplace_back(std::move(columnName),
                                            value);
                        }
                    }

                    return true;
                };
            
            clauseStorage.forEachAtom(recordAtom);
            
        }
    }
    return { std::move(outputValues), std::move(extra) };
}

} // namespace MLDB
