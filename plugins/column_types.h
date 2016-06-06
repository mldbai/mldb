/** column_types.h                                       -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    Structure to record types and ranges of values in a column to aid
    with determining its size.
*/

#pragma once

#include <memory>
#include "mldb/types/value_description_fwd.h"

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
    uint64_t numZeros;
    
    uint64_t numIntegers;
    int64_t minNegativeInteger;
    int64_t maxNegativeInteger;
    uint64_t minPositiveInteger;
    uint64_t maxPositiveInteger;

    bool hasPositiveIntegers() const
    {
        return numIntegers && maxPositiveInteger >= minPositiveInteger;
    }

    bool hasNegativeIntegers() const
    {
        return numIntegers && maxNegativeInteger >= minNegativeInteger;
    }

    bool onlyIntegers() const
    {
        return numNulls == 0 && onlyIntegersAndNulls();
    }

    bool onlyIntegersAndNulls() const
    {
        return numReals == 0 && numStrings == 0 && numBlobs == 0 && numOther == 0;
    }

    /// Are there only double-valued numbers (reals, and integers which
    /// can be represented exactly as a double)?
    bool onlyExactDoubles() const
    {
        return numNulls == 0 && onlyExactDoublesAndNulls();
    }

    /// Are there only double-valued numbers (reals, and integers which
    /// can be represented exactly as a double) as well as nulls?
    bool onlyExactDoublesAndNulls() const
    {
        if (numStrings || numBlobs || numOther)
            return false;
        if (hasPositiveIntegers()) {
            if (maxPositiveInteger > (1ULL << 53))
                return false;  // out of range for exact double
        }
        if (hasNegativeIntegers()) {
            if (minNegativeInteger < -(1LL << 53))
                return false;  // out of range for exact double
        }
        return true;
    }

    uint64_t numReals;
    uint64_t numStrings;
    uint64_t numBlobs;
    uint64_t numOther;  // timestamps, intervals
};

DECLARE_STRUCTURE_DESCRIPTION(ColumnTypes);

} // namespace MLDB
} // namespace Datacratic


