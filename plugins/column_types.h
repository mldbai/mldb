/** column_types.h                                       -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    Structure to record types and ranges of values in a column to aid
    with determining its size.
*/

#pragma once

#include "mldb/sql/cell_value.h"

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* COLUMN TYPES                                                              */
/*****************************************************************************/
   
struct ColumnTypes {
    ColumnTypes()
        : numNulls(false), numIntegers(false),
          minNegativeInteger(0), maxPositiveInteger(0),
          numReals(false), numStrings(false), numBlobs(false),
          numOther(false)
    {
    }

    void update(const CellValue & val)
    {
        // Record the type
        switch (val.cellType()) {
        case CellValue::EMPTY:
            numNulls += 1;  break;
        case CellValue::FLOAT:
            numReals += 1;  break;

        case CellValue::INTEGER:
            numIntegers += 1;
            if (val.isUInt64()) {
                maxPositiveInteger = std::max(maxPositiveInteger, val.toUInt());
            }
            else {
                minNegativeInteger = std::min(minNegativeInteger, val.toInt());
            }
            break;
        case CellValue::ASCII_STRING:
        case CellValue::UTF8_STRING:
            numStrings += 1;  break;
        case CellValue::BLOB:
            numBlobs += 1;  break;
        default:
            numOther += 1;  break;
        }
    }

    void update(const ColumnTypes & other)
    {
        numNulls = numNulls + other.numNulls;
        numIntegers = numIntegers + other.numIntegers;
        minNegativeInteger
            = std::min(minNegativeInteger, other.minNegativeInteger);
        maxPositiveInteger
            = std::max(maxPositiveInteger, other.maxPositiveInteger);
        numReals = numReals + other.numReals;
        numStrings = numStrings + other.numStrings;
        numBlobs = numBlobs + other.numBlobs;
        numOther = numOther + other.numOther;
    }

    std::shared_ptr<ExpressionValueInfo>
    getExpressionValueInfo() const
    {
        if (!numNulls && !numReals && !numStrings && !numOther) {
            // Integers only
            if (minNegativeInteger == 0) {
                // All positive
                return std::make_shared<Uint64ValueInfo>();
            }
            else if (maxPositiveInteger <= (1ULL << 63)) {
                // Fits in a 64 bit integer
                return std::make_shared<IntegerValueInfo>();
            }
            else {
                // Out of range of either positive or negative integers
                // only.  We say it's an atom.
                return std::make_shared<AtomValueInfo>();
            }
        }
        else if (!numNulls && !numStrings && !numOther) {
            // Reals and integers.  If all integers are representable as
            // doubles, in other words a maximum of 53 bits, then we're all
            // doubles.
            if (maxPositiveInteger < (1ULL << 53)
                && minNegativeInteger > -(1LL << 53)) {
                return std::make_shared<Float64ValueInfo>();
            }
            // Doubles would lose precision.  It's an atom.
            return std::make_shared<AtomValueInfo>();
        }
        else if (!numNulls && !numIntegers && !numReals && !numOther) {
            return std::make_shared<Utf8StringValueInfo>();
        }
        else {
            return std::make_shared<AtomValueInfo>();
        }
    }
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


