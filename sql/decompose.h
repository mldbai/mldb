/** decompose.h                                               -*- C++ -*-
    Jeremy Barnes, 24 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Base SQL expression support.
*/

#pragma once


#include <vector>
#include "sql_expression.h"
#include "mldb/utils/lightweight_hash.h"


namespace MLDB {

// Describes any operations that transform a single column into
// another single column; these can be run in-place.
// This is in order of the input columns; only single input to
// single output operations are run.
struct ColumnOperation {
    int clauseNum = -1;      ///< Which index in decomposed we belong to?
    int outCol = -1;         ///< Output column number
    std::vector<int> inputCols;
    bool moveInputs = false;   ///< Can we move inputs into place?
    BoundSqlExpression bound;  ///< Expression to compute column; null mesans
};

// We split into three different types of operations, run in
// order:
// 1.  Clauses that can't be optimized generically
// 2.  Clauses that run a function over the inputs returning a single col
// 3.a Clauses that copy a single input to a single output
// 3.b Clauses that move a single intput to a single output

struct Decomposition {
    std::vector<BoundSqlExpression> otherClauses;
    std::vector<ColumnOperation> ops;
    std::vector<ColumnPath> knownColumnNames;
    LightweightHash<ColumnHash, int> inputColumnIndex;
    LightweightHash<ColumnHash, int> columnIndex; //To check for duplicates column names
    bool canUseDecomposed = false;

    std::tuple<std::vector<CellValue>, std::vector<std::pair<ColumnPath, CellValue>>>
    apply(SqlRowScope & row, std::span<const CellValue> values) const;
};

Decomposition decompose(const BoundSqlExpression & selectBound,
                        SqlBindingScope & scope);

} // namespace MLDB
