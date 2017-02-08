/** column_types.cc
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Code to determine how to store a column based upon its contents.
*/

#include "column_types.h"
#include "mldb/sql/cell_value.h"
#include "mldb/sql/expression_value.h"
#include "mldb/types/structure_description.h"



namespace MLDB {


/*****************************************************************************/
/* COLUMN TYPES                                                              */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(ColumnTypes);

ColumnTypesDescription::
ColumnTypesDescription()
{
    addField("numNulls", &ColumnTypes::numNulls,
             "Number of null values in the column");
    addField("numZeros", &ColumnTypes::numZeros,
             "Number of zero values in the column");
    addField("numIntegers", &ColumnTypes::numIntegers,
             "Number of 64 bit integral values in the column");
    addField("minNegativeInteger", &ColumnTypes::minNegativeInteger,
             "Minimum negative integer value in the column",
             std::numeric_limits<int64_t>::max());
    addField("maxNegativeInteger", &ColumnTypes::maxNegativeInteger,
             "Maximum negative integer value in the column", (int64_t)0);
    addField("minPositiveInteger", &ColumnTypes::minPositiveInteger,
             "Minimum positive integer value in the column",
             std::numeric_limits<uint64_t>::max());
    addField("maxPositiveInteger", &ColumnTypes::maxPositiveInteger,
             "Maximum positive integer value in the column", (uint64_t)0);
    addField("numReals", &ColumnTypes::numReals,
             "Number of real-valued (non-64 bit integral) values in the column");
    addField("numStrings", &ColumnTypes::numStrings,
             "Number of string values in the column");
    addField("numBlobs", &ColumnTypes::numBlobs,
             "Number of blob values in the column");
    addField("numOther", &ColumnTypes::numOther,
             "Number of other typed values in the column");
}

/** This is an accumulator that keeps statistics on the types of values that
    a column could have.  It's useful for knowing how to treat a column in
    an algorithm.
*/

ColumnTypes::   
ColumnTypes()
    : numNulls(0), numZeros(0), numIntegers(0),
      minNegativeInteger(std::numeric_limits<int64_t>::max()),
      maxNegativeInteger(0),
      minPositiveInteger(std::numeric_limits<uint64_t>::max()),
      maxPositiveInteger(0),
      numReals(0), numStrings(0), numBlobs(0),
      numOther(0)
{
}

void
ColumnTypes::   
update(const CellValue & val)
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
            uint64_t i = val.toUInt();
            numZeros += (i == 0);
            minPositiveInteger = std::min(minPositiveInteger, i);
            maxPositiveInteger = std::max(maxPositiveInteger, i);
        }
        else {
            int64_t i = val.toInt();
            numZeros += (i == 0);
            minNegativeInteger = std::min(minNegativeInteger, i);
            maxNegativeInteger = std::max(maxNegativeInteger, i);
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

void
ColumnTypes::   
update(const ColumnTypes & other)
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
ColumnTypes::   
getExpressionValueInfo() const
{
    if (!numNulls && !numReals && !numStrings && !numBlobs && !numOther) {
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
    else if (!numNulls && !numStrings && !numBlobs && !numOther) {
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
    else if (!numNulls && !numIntegers && !numReals && !numBlobs && !numOther) {
        return std::make_shared<Utf8StringValueInfo>();
    }
    else {
        return std::make_shared<AtomValueInfo>();
    }
}

} // namespace MLDB

