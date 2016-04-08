/** column_types.h                                       -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    Structure to record types and ranges of values in a column to aid
    with determining its size.
*/

#pragma once

#include <memory>

namespace Datacratic {
namespace MLDB {

struct CellValue;
struct ExpressionValueInfo;

/*****************************************************************************/
/* COLUMN TYPES                                                              */
/*****************************************************************************/
   
/** This is an accumulator that keeps statistics on the types of values that
    a column could have.  It's useful for knowing how to treat a column in
    an algorithm.
*/

struct ColumnTypes {
    ColumnTypes();

    void update(const CellValue & val);

    void update(const ColumnTypes & other);

    std::shared_ptr<ExpressionValueInfo>
    getExpressionValueInfo() const;

    uint64_t numNulls;

    uint64_t numIntegers;
    int64_t minNegativeInteger;
    uint64_t maxPositiveInteger;

    uint64_t numReals;
    uint64_t numStrings;
    uint64_t numBlobs;
    uint64_t numOther;  // timestamps, intervals
};

} // namespace MLDB
} // namespace Datacratic


