/** column_types.h                                       -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    Structure to record types and ranges of values in a column to aid
    with determining its size.
*/

#pragma once

#include <memory>
#include <limits>
#include "mldb/types/value_description_fwd.h"


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

    uint64_t numNulls = 0;
    uint64_t numZeros = 0;
    
    uint64_t numIntegers = 0;
    int64_t minNegativeInteger = std::numeric_limits<int64_t>::max();
    int64_t maxNegativeInteger = 0;
    uint64_t minPositiveInteger = std::numeric_limits<uint64_t>::max();
    uint64_t maxPositiveInteger = 0;

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
        return numReals == 0 && numStrings == 0 && numBlobs == 0
            && numPaths == 0 && numOther == 0;
    }

    uint64_t numReals = 0;
    uint64_t numStrings = 0;
    uint64_t numBlobs = 0;
    uint64_t numPaths = 0;
    uint64_t numOther = 0;  // timestamps, intervals
};

DECLARE_STRUCTURE_DESCRIPTION(ColumnTypes);

} // namespace MLDB



